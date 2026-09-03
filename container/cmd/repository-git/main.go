package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultListenAddress = ":8080"
	bridgeBaseURL        = "http://repo-r2.internal/r2/"
	maxRequestBytes      = 1 << 20
	maxActivePacks       = 250
	maxCommands          = 4096
	maxHydratedBytes     = int64(6_500_000_000)
	packRefMagic         = uint32(0x50524546)
	packRefVersion       = uint32(1)
	packRefHeaderBytes   = 4 + 4 + 4 + 8 + 20 + 20
	objectIDBytes        = 20
	containerHTTPClient  = 30 * time.Minute
	persistentCacheRoot  = "/tmp/repository-git-cache"
	maxStockProcessSlots = 8
)

func listenAddress() string {
	configured := os.Getenv("REPOSITORY_GIT_LISTEN_ADDRESS")
	if configured == "" {
		return defaultListenAddress
	}
	host, port, err := net.SplitHostPort(configured)
	if err != nil || host != "127.0.0.1" {
		return defaultListenAddress
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1024 || parsedPort > 65535 {
		return defaultListenAddress
	}
	return configured
}

var (
	objectIDPattern      = regexp.MustCompile(`^[0-9a-f]{40}$`)
	refNamePattern       = regexp.MustCompile(`^refs/[^\x00-\x20~^:?*\\\[]+$`)
	processSlot          = make(chan struct{}, 1)
	stockProcessSlots    = make(chan struct{}, maxStockProcessSlots)
	maintenanceStatusMu  sync.RWMutex
	maintenanceOperation string
)

type packInput struct {
	PackKey   string `json:"packKey"`
	PackBytes int64  `json:"packBytes"`
	IdxBytes  int64  `json:"idxBytes"`
}

type receiveCommand struct {
	OldOID string `json:"oldOid"`
	NewOID string `json:"newOid"`
	Ref    string `json:"ref"`
}

type processRequest struct {
	OperationID   string           `json:"operationId"`
	InputPackKey  string           `json:"inputPackKey"`
	InputBytes    int64            `json:"inputBytes"`
	ActivePacks   []packInput      `json:"activePacks"`
	Commands      []receiveCommand `json:"commands"`
	OutputPackKey string           `json:"outputPackKey"`
	OutputIdxKey  string           `json:"outputIdxKey"`
	OutputRefsKey string           `json:"outputRefsKey"`
	Maintenance   *gcIndexRequest  `json:"maintenance,omitempty"`
}

type processResponse struct {
	OperationID     string         `json:"operationId"`
	PackBytes       int64          `json:"packBytes"`
	IdxBytes        int64          `json:"idxBytes"`
	RefsBytes       int64          `json:"refsBytes"`
	ObjectCount     uint32         `json:"objectCount"`
	PackSHA1        string         `json:"packSha1"`
	ElapsedMS       int64          `json:"elapsedMs"`
	ScratchBytes    int64          `json:"scratchBytes"`
	HydratedBytes   int64          `json:"hydratedBytes"`
	DownloadedBytes int64          `json:"downloadedBytes"`
	CacheHitBytes   int64          `json:"cacheHitBytes"`
	Maintenance     *gcIndexResult `json:"maintenance,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

type transientProcessError struct {
	cause error
}

func (failure transientProcessError) Error() string {
	return failure.cause.Error()
}

func (failure transientProcessError) Unwrap() error {
	return failure.cause
}

func transientFailure(message string, err error) error {
	return transientProcessError{cause: fmt.Errorf("%s: %w", message, err)}
}

type bridgeUploadStatusError struct{ status int }

func (failure bridgeUploadStatusError) Error() string {
	return fmt.Sprintf("bridge PUT returned %d", failure.status)
}

func outputUploadFailure(input processRequest, message string, err error) error {
	var rejected bridgeUploadStatusError
	if input.Maintenance != nil && errors.As(err, &rejected) {
		switch rejected.status {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusRequestEntityTooLarge:
			return fmt.Errorf("%s: %w", message, err)
		}
	}
	return transientFailure(message, err)
}

type bridgeClient struct {
	client  *http.Client
	baseURL string
}

type objectBridge interface {
	download(context.Context, string, string, int64) error
	upload(context.Context, string, string, int64) error
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "stock-pre-receive" {
		if err := runStockPreReceiveHook(context.Background(), os.Stdin); err != nil {
			fmt.Fprintln(os.Stderr, "selective receive closure rejected")
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "stock-receive-stdio" {
		if err := runStockReceiveStdio(context.Background(), os.Stdin, os.Stdout); err != nil {
			code := "stock-receive-rejected"
			var transient transientProcessError
			if errors.As(err, &transient) {
				code = "r2-transient"
			}
			_ = json.NewEncoder(os.Stderr).Encode(errorResponse{
				Error: "stock receive failed",
				Code:  code,
			})
			os.Exit(1)
		}
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ready\n"))
	})
	mux.HandleFunc("POST /process", processHandler)
	mux.HandleFunc("GET /maintenance/status", maintenanceStatusHandler)
	mux.HandleFunc("POST /stock-receive", stockReceiveHandler)
	mux.HandleFunc("POST /stock-receive-bundle", stockReceiveBundleHandler)

	server := &http.Server{
		Addr:              listenAddress(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(os.Stderr, "repository-git: server failed: %v\n", err)
		os.Exit(1)
	}
}

func processHandler(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()

	var input processRequest
	if err := decoder.Decode(&input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid process request")
		return
	}
	if err := validateProcessRequest(input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !acquireProcessSlot() {
		writeProcessError(writer, transientFailure("repository processor busy", errors.New("another operation is active")))
		return
	}
	defer releaseProcessSlot()
	processContext := request.Context()
	if input.Maintenance != nil {
		var cancel context.CancelFunc
		processContext, cancel = context.WithTimeout(processContext, 14*time.Minute)
		defer cancel()
		maintenanceStatusMu.Lock()
		maintenanceOperation = input.OperationID
		maintenanceStatusMu.Unlock()
		defer func() {
			maintenanceStatusMu.Lock()
			maintenanceOperation = ""
			maintenanceStatusMu.Unlock()
		}()
	}

	startedAt := time.Now()
	result, err := processPack(processContext, input)
	if err != nil {
		// Native Git errors can contain repository OIDs, refs, and paths. Emit
		// only a closed category to provider stderr; the HTTP response is also
		// deliberately generic.
		fmt.Fprintf(os.Stderr, "repository-git: operation failed: %s\n", processErrorCategory(err))
		writeProcessError(writer, err)
		return
	}
	result.ElapsedMS = time.Since(startedAt).Milliseconds()
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(result)
}

func maintenanceStatusHandler(writer http.ResponseWriter, _ *http.Request) {
	maintenanceStatusMu.RLock()
	operation := maintenanceOperation
	maintenanceStatusMu.RUnlock()
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(struct {
		OperationID string `json:"operationId"`
	}{operation})
}

func acquireProcessSlot() bool {
	select {
	case processSlot <- struct{}{}:
		return true
	default:
		return false
	}
}

func acquireStockProcessSlot() bool {
	select {
	case stockProcessSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func releaseStockProcessSlot() {
	<-stockProcessSlots
}

func releaseProcessSlot() {
	<-processSlot
}

func processErrorCategory(err error) string {
	var transient transientProcessError
	if errors.As(err, &transient) {
		return "transient"
	}
	return "rejected"
}

func writeProcessError(writer http.ResponseWriter, err error) {
	var transient transientProcessError
	if errors.As(err, &transient) {
		writeError(writer, http.StatusServiceUnavailable, "native_git_transient", "native Git processing is temporarily unavailable")
		return
	}
	writeError(writer, http.StatusUnprocessableEntity, "native_git_failed", "native Git processing failed")
}

func validateProcessRequest(input processRequest) error {
	if input.OperationID == "" || len(input.OperationID) > 200 {
		return errors.New("invalid operation id")
	}
	if input.InputPackKey == "" || input.InputBytes <= 0 {
		return errors.New("invalid input pack")
	}
	if len(input.ActivePacks) > maxActivePacks {
		return errors.New("active pack catalog exceeds processor capacity")
	}
	if input.Maintenance != nil {
		if err := validateGcIndexRequest(input); err != nil {
			return err
		}
	} else if len(input.Commands) == 0 || len(input.Commands) > maxCommands {
		return errors.New("invalid receive command count")
	}
	if input.OutputPackKey == "" || input.OutputIdxKey == "" || input.OutputRefsKey == "" {
		return errors.New("missing output key")
	}

	totalBytes := input.InputBytes
	if totalBytes > maxHydratedBytes {
		return errors.New("repository exceeds processor scratch capacity")
	}
	for _, pack := range input.ActivePacks {
		if pack.PackKey == "" || pack.PackBytes <= 0 || pack.IdxBytes <= 0 {
			return errors.New("invalid active pack")
		}
		totalBytes += pack.PackBytes + pack.IdxBytes
		if totalBytes > maxHydratedBytes {
			return errors.New("repository exceeds processor scratch capacity")
		}
	}
	for _, command := range input.Commands {
		if !objectIDPattern.MatchString(command.OldOID) || !objectIDPattern.MatchString(command.NewOID) {
			return errors.New("invalid command object id")
		}
		if !refNamePattern.MatchString(command.Ref) || strings.Contains(command.Ref, "..") || strings.HasSuffix(command.Ref, ".") {
			return errors.New("invalid command ref")
		}
	}
	return nil
}

func processPack(ctx context.Context, input processRequest) (processResponse, error) {
	client := bridgeClient{
		client: &http.Client{
			Timeout:   containerHTTPClient,
			Transport: &http.Transport{DisableKeepAlives: true},
		},
		baseURL: bridgeBaseURL,
	}
	return processPackWithBridgeAtCache(ctx, input, client, persistentCacheRoot)
}

func processPackWithBridge(ctx context.Context, input processRequest, client objectBridge) (processResponse, error) {
	cacheRoot, err := os.MkdirTemp("", "repository-git-cache-test-")
	if err != nil {
		return processResponse{}, err
	}
	defer os.RemoveAll(cacheRoot)
	return processPackWithBridgeAtCache(ctx, input, client, cacheRoot)
}

type hydrationMetrics struct {
	hydratedBytes   int64
	downloadedBytes int64
	cacheHitBytes   int64
	cacheHitPaths   []string
}

func cachePath(cacheRoot string, key string, expectedBytes int64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", key, expectedBytes)))
	return filepath.Join(cacheRoot, hex.EncodeToString(digest[:]))
}

func ensureCachedArtifact(ctx context.Context, client objectBridge, cacheRoot string, key string, expectedBytes int64) (string, bool, error) {
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		return "", false, err
	}
	path := cachePath(cacheRoot, key, expectedBytes)
	if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() && info.Size() == expectedBytes {
		return path, true, nil
	} else if err == nil {
		if removeErr := os.Remove(path); removeErr != nil {
			return "", false, removeErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	temporary, err := os.CreateTemp(cacheRoot, ".hydrate-")
	if err != nil {
		return "", false, err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return "", false, err
	}
	if err := os.Remove(temporaryPath); err != nil {
		return "", false, err
	}
	defer os.Remove(temporaryPath)
	if err := client.download(ctx, key, temporaryPath, expectedBytes); err != nil {
		return "", false, err
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return "", false, err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return "", false, err
	}
	return path, false, nil
}

func linkOrCopy(source string, destination string) error {
	if err := os.Link(source, destination); err == nil {
		return nil
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		_ = os.Remove(destination)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(destination)
		return closeErr
	}
	return nil
}

func hydrateArtifact(ctx context.Context, client objectBridge, cacheRoot string, key string, expectedBytes int64, destination string, metrics *hydrationMetrics) error {
	cachedPath, hit, err := ensureCachedArtifact(ctx, client, cacheRoot, key, expectedBytes)
	if err != nil {
		if downloadErr := client.download(ctx, key, destination, expectedBytes); downloadErr != nil {
			return downloadErr
		}
		metrics.hydratedBytes += expectedBytes
		metrics.downloadedBytes += expectedBytes
		return nil
	}
	metrics.hydratedBytes += expectedBytes
	if hit {
		metrics.cacheHitBytes += expectedBytes
		metrics.cacheHitPaths = append(metrics.cacheHitPaths, cachedPath)
	} else {
		metrics.downloadedBytes += expectedBytes
	}
	return linkOrCopy(cachedPath, destination)
}

func evictCacheHits(metrics hydrationMetrics) {
	for _, path := range metrics.cacheHitPaths {
		_ = os.Remove(path)
	}
}

func failAfterHydration(metrics hydrationMetrics, stage string, err error) error {
	if metrics.cacheHitBytes == 0 {
		return err
	}
	evictCacheHits(metrics)
	return transientFailure(stage, err)
}

func retainOutputInCache(cacheRoot string, key string, expectedBytes int64, source string) error {
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		return err
	}
	destination := cachePath(cacheRoot, key, expectedBytes)
	if info, err := os.Stat(destination); err == nil && info.Mode().IsRegular() && info.Size() == expectedBytes {
		return nil
	} else if err == nil {
		if removeErr := os.Remove(destination); removeErr != nil {
			return removeErr
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(cacheRoot, ".publish-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	defer os.Remove(temporaryPath)
	if err := linkOrCopy(source, temporaryPath+"-linked"); err != nil {
		return err
	}
	linkedPath := temporaryPath + "-linked"
	defer os.Remove(linkedPath)
	if err := os.Rename(linkedPath, destination); err != nil {
		return err
	}
	return nil
}

func pruneArtifactCache(cacheRoot string, activePacks []packInput) error {
	if err := os.MkdirAll(cacheRoot, 0o700); err != nil {
		return err
	}
	retained := make(map[string]struct{}, len(activePacks)*2)
	for _, pack := range activePacks {
		retained[filepath.Base(cachePath(cacheRoot, pack.PackKey, pack.PackBytes))] = struct{}{}
		indexKey := strings.TrimSuffix(pack.PackKey, ".pack") + ".idx"
		retained[filepath.Base(cachePath(cacheRoot, indexKey, pack.IdxBytes))] = struct{}{}
	}
	entries, err := os.ReadDir(cacheRoot)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, keep := retained[entry.Name()]; keep {
			continue
		}
		if err := os.Remove(filepath.Join(cacheRoot, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func processPackWithBridgeAtCache(ctx context.Context, input processRequest, client objectBridge, cacheRoot string) (processResponse, error) {
	startedAt := time.Now()
	phases := gcIndexResult{}
	_ = pruneArtifactCache(cacheRoot, input.ActivePacks)
	workDir, err := os.MkdirTemp("", "repository-git-")
	if err != nil {
		return processResponse{}, err
	}
	defer os.RemoveAll(workDir)

	repoDir := filepath.Join(workDir, "repo.git")
	if err := runGit(ctx, "", "init", "--bare", repoDir); err != nil {
		return processResponse{}, err
	}
	packDir := filepath.Join(repoDir, "objects", "pack")
	metrics := hydrationMetrics{}
	for index, pack := range input.ActivePacks {
		base := filepath.Join(packDir, fmt.Sprintf("pack-active-%04d", index))
		if err := hydrateArtifact(ctx, client, cacheRoot, pack.PackKey, pack.PackBytes, base+".pack", &metrics); err != nil {
			return processResponse{}, transientFailure("download active pack", err)
		}
		if err := hydrateArtifact(ctx, client, cacheRoot, strings.TrimSuffix(pack.PackKey, ".pack")+".idx", pack.IdxBytes, base+".idx", &metrics); err != nil {
			return processResponse{}, transientFailure("download active index", err)
		}
	}

	inputPath := filepath.Join(workDir, "input.pack")
	phaseStartedAt := time.Now()
	if err := client.download(ctx, input.InputPackKey, inputPath, input.InputBytes); err != nil {
		return processResponse{}, transientFailure("download input pack", err)
	}
	metrics.downloadedBytes += input.InputBytes
	phases.DownloadMS = time.Since(phaseStartedAt).Milliseconds()
	existingPacks, err := listPackFiles(packDir)
	if err != nil {
		return processResponse{}, failAfterHydration(metrics, "inspect hydrated packs", err)
	}
	phaseStartedAt = time.Now()
	if err := indexPack(ctx, repoDir, inputPath); err != nil {
		return processResponse{}, failAfterHydration(metrics, "index input against hydrated packs", err)
	}
	phases.IndexMS = time.Since(phaseStartedAt).Milliseconds()

	packPath, idxPath, err := findProducedPack(packDir, existingPacks)
	if err != nil {
		return processResponse{}, failAfterHydration(metrics, "locate indexed pack", err)
	}
	phaseStartedAt = time.Now()
	if input.Maintenance != nil {
		if err := verifyGcIndex(ctx, repoDir, packPath, idxPath, input); err != nil {
			return processResponse{}, failAfterHydration(metrics, "verify maintenance closure", err)
		}
	} else {
		if err := verifyConnectivity(ctx, repoDir, input.Commands); err != nil {
			return processResponse{}, failAfterHydration(metrics, "verify hydrated connectivity", err)
		}
	}
	if err := runGit(ctx, repoDir, "verify-pack", "-s", idxPath); err != nil {
		return processResponse{}, failAfterHydration(metrics, "verify indexed pack", err)
	}
	phases.ValidationMS = time.Since(phaseStartedAt).Milliseconds()

	packBytes, err := fileSize(packPath)
	if err != nil {
		return processResponse{}, failAfterHydration(metrics, "inspect output pack", err)
	}
	idxBytes, err := fileSize(idxPath)
	if err != nil {
		return processResponse{}, failAfterHydration(metrics, "inspect output index", err)
	}
	refsPath := filepath.Join(workDir, "output.refs")
	phaseStartedAt = time.Now()
	objectCount, packSHA1, err := buildPackReferenceIndex(ctx, repoDir, packPath, idxPath, refsPath)
	if err != nil {
		return processResponse{}, failAfterHydration(metrics, "build output reference index", err)
	}
	refsBytes, err := fileSize(refsPath)
	if err != nil {
		return processResponse{}, failAfterHydration(metrics, "inspect output reference index", err)
	}
	phases.ReferenceMS = time.Since(phaseStartedAt).Milliseconds()

	phaseStartedAt = time.Now()
	if err := client.upload(ctx, input.OutputPackKey, packPath, packBytes); err != nil {
		return processResponse{}, outputUploadFailure(input, "upload output pack", err)
	}
	if err := client.upload(ctx, input.OutputIdxKey, idxPath, idxBytes); err != nil {
		return processResponse{}, outputUploadFailure(input, "upload output index", err)
	}
	if err := client.upload(ctx, input.OutputRefsKey, refsPath, refsBytes); err != nil {
		return processResponse{}, outputUploadFailure(input, "upload output refs", err)
	}
	phases.UploadMS = time.Since(phaseStartedAt).Milliseconds()
	_ = retainOutputInCache(cacheRoot, input.OutputPackKey, packBytes, packPath)
	_ = retainOutputInCache(cacheRoot, input.OutputIdxKey, idxBytes, idxPath)
	scratchBytes, _ := directoryBytes(workDir)

	result := processResponse{
		OperationID:     input.OperationID,
		PackBytes:       packBytes,
		IdxBytes:        idxBytes,
		RefsBytes:       refsBytes,
		ObjectCount:     objectCount,
		PackSHA1:        packSHA1,
		ScratchBytes:    scratchBytes,
		HydratedBytes:   metrics.hydratedBytes,
		DownloadedBytes: metrics.downloadedBytes,
		CacheHitBytes:   metrics.cacheHitBytes,
		ElapsedMS:       time.Since(startedAt).Milliseconds(),
	}
	if input.Maintenance != nil {
		phases.ObjectSetSHA256 = input.Maintenance.ObjectSetSHA256
		phases.DownloadBytes = input.InputBytes
		phases.UploadBytes = packBytes + idxBytes + refsBytes
		phases.DownloadRequests = 1
		phases.UploadRequests = 3
		result.Maintenance = &phases
		// Write the completion receipt last. Recovery reads this immutable
		// result before deciding whether another native execution is needed.
		if err := uploadGcResult(ctx, client, workDir, input.Maintenance.ResultKey, result); err != nil {
			return processResponse{}, outputUploadFailure(input, "upload maintenance receipt", err)
		}
	}
	return result, nil
}

func directoryBytes(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func indexPack(ctx context.Context, repoDir string, inputPath string) error {
	input, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer input.Close()

	command := exec.CommandContext(ctx, "git", "index-pack", "--stdin", "--fix-thin", "--keep=display-receive")
	command.Dir = repoDir
	command.Env = append(os.Environ(), "GIT_DIR="+repoDir)
	command.Stdin = input
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git index-pack failed: %w: %s", err, boundedText(output))
	}
	return nil
}

func listPackFiles(packDir string) (map[string]struct{}, error) {
	packFiles, err := filepath.Glob(filepath.Join(packDir, "pack-*.pack"))
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(packFiles))
	for _, packPath := range packFiles {
		result[packPath] = struct{}{}
	}
	return result, nil
}

func findProducedPack(packDir string, existing map[string]struct{}) (string, string, error) {
	packFiles, err := filepath.Glob(filepath.Join(packDir, "pack-*.pack"))
	if err != nil {
		return "", "", err
	}
	produced := make([]string, 0, 1)
	for _, packPath := range packFiles {
		if _, found := existing[packPath]; !found {
			produced = append(produced, packPath)
		}
	}
	if len(produced) != 1 {
		return "", "", fmt.Errorf("expected one produced pack, found %d", len(produced))
	}
	idxPath := strings.TrimSuffix(produced[0], ".pack") + ".idx"
	if _, err := os.Stat(idxPath); err != nil {
		return "", "", fmt.Errorf("produced index missing: %w", err)
	}
	return produced[0], idxPath, nil
}

func verifyConnectivity(ctx context.Context, repoDir string, commands []receiveCommand) error {
	for _, command := range commands {
		if err := runGit(ctx, repoDir, "check-ref-format", command.Ref); err != nil {
			return fmt.Errorf("invalid command ref: %w", err)
		}
		if isZeroOID(command.NewOID) {
			continue
		}
		if err := runGit(ctx, repoDir, "cat-file", "-e", command.NewOID+"^{object}"); err != nil {
			return fmt.Errorf("new ref object missing: %w", err)
		}
		if err := runGit(ctx, repoDir, "update-ref", command.Ref, command.NewOID); err != nil {
			return fmt.Errorf("temporary ref update failed: %w", err)
		}
	}
	if err := runGit(ctx, repoDir, "fsck", "--connectivity-only", "--no-dangling"); err != nil {
		return fmt.Errorf("git connectivity check failed: %w", err)
	}
	return nil
}

func runGit(ctx context.Context, gitDir string, args ...string) error {
	command := exec.CommandContext(ctx, "git", args...)
	if gitDir != "" {
		command.Dir = gitDir
	}
	command.Env = stockGitEnvironment(ctx, gitDir)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s failed: %w: %s", args[0], err, boundedText(output))
	}
	return nil
}

func boundedText(value []byte) string {
	const maximum = 2048
	if len(value) > maximum {
		value = value[:maximum]
	}
	return strings.TrimSpace(string(value))
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func isZeroOID(oid string) bool {
	return oid == strings.Repeat("0", 40)
}

func (bridge bridgeClient) url(key string) string {
	return bridge.baseURL + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func (bridge bridgeClient) download(ctx context.Context, key string, destination string, expectedBytes int64) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, bridge.url(key), nil)
	if err != nil {
		return err
	}
	response, err := bridge.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("bridge GET returned %d", response.StatusCode)
	}

	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(file, io.LimitReader(response.Body, expectedBytes+1))
	closeErr := file.Close()
	if copyErr != nil {
		return fmt.Errorf("bridge GET body failed after %d of %d bytes: %w", written, expectedBytes, copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if written != expectedBytes {
		return fmt.Errorf("bridge GET size mismatch: expected %d, got %d", expectedBytes, written)
	}
	return nil
}

func (bridge bridgeClient) upload(ctx context.Context, key string, source string, size int64) error {
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	defer file.Close()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, bridge.url(key), file)
	if err != nil {
		return err
	}
	request.ContentLength = size
	request.Header.Set("Content-Type", "application/octet-stream")
	if digest, digestErr := sha256File(source); digestErr == nil {
		request.Header.Set("X-Display-SHA256", digest)
	} else {
		return digestErr
	}
	response, err := bridge.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return bridgeUploadStatusError{status: response.StatusCode}
	}
	return nil
}

func writeError(writer http.ResponseWriter, status int, code string, message string) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(errorResponse{Error: message, Code: code})
}

func buildPackReferenceIndex(ctx context.Context, repoDir string, packPath string, idxPath string, outputPath string) (uint32, string, error) {
	idxBytes, err := os.ReadFile(idxPath)
	if err != nil {
		return 0, "", err
	}
	oids, packChecksum, idxChecksum, err := parseIndex(idxBytes)
	if err != nil {
		return 0, "", err
	}
	packSize, err := fileSize(packPath)
	if err != nil {
		return 0, "", err
	}

	typeCodes := make([]byte, len(oids))
	refStarts := make([]uint32, len(oids)+1)
	refsFile, err := os.CreateTemp(filepath.Dir(outputPath), "pack-refs-")
	if err != nil {
		return 0, "", err
	}
	refsPath := refsFile.Name()
	defer os.Remove(refsPath)

	refCount, err := streamObjectMetadata(ctx, repoDir, oids, typeCodes, refStarts, refsFile)
	if closeErr := refsFile.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, "", err
	}

	output, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, "", err
	}
	defer output.Close()
	if err := writePackRefHeader(output, uint32(len(oids)), uint64(packSize), packChecksum, idxChecksum); err != nil {
		return 0, "", err
	}
	if _, err := output.Write(typeCodes); err != nil {
		return 0, "", err
	}
	for _, start := range refStarts {
		if err := binary.Write(output, binary.BigEndian, start); err != nil {
			return 0, "", err
		}
	}
	refsInput, err := os.Open(refsPath)
	if err != nil {
		return 0, "", err
	}
	defer refsInput.Close()
	if _, err := io.Copy(output, refsInput); err != nil {
		return 0, "", err
	}
	if refCount != refStarts[len(refStarts)-1] {
		return 0, "", errors.New("reference count mismatch")
	}
	return uint32(len(oids)), hex.EncodeToString(packChecksum), nil
}

func parseIndex(data []byte) ([][]byte, []byte, []byte, error) {
	minimum := 8 + 256*4 + 40
	if len(data) < minimum || !bytes.Equal(data[:4], []byte{0xff, 't', 'O', 'c'}) {
		return nil, nil, nil, errors.New("unsupported pack index")
	}
	if binary.BigEndian.Uint32(data[4:8]) != 2 {
		return nil, nil, nil, errors.New("unsupported pack index version")
	}
	count := binary.BigEndian.Uint32(data[8+255*4 : 8+256*4])
	oidStart := 8 + 256*4
	oidEnd := oidStart + int(count)*objectIDBytes
	if oidEnd > len(data)-40 {
		return nil, nil, nil, errors.New("truncated pack index object ids")
	}
	expectedIdxChecksum := sha1.Sum(data[:len(data)-20])
	if !bytes.Equal(expectedIdxChecksum[:], data[len(data)-20:]) {
		return nil, nil, nil, errors.New("pack index checksum mismatch")
	}
	oids := make([][]byte, count)
	for index := uint32(0); index < count; index++ {
		start := oidStart + int(index)*objectIDBytes
		oids[index] = append([]byte(nil), data[start:start+objectIDBytes]...)
	}
	return oids, append([]byte(nil), data[len(data)-40:len(data)-20]...), append([]byte(nil), data[len(data)-20:]...), nil
}

func streamObjectMetadata(ctx context.Context, repoDir string, oids [][]byte, typeCodes []byte, refStarts []uint32, refsFile *os.File) (uint32, error) {
	command := exec.CommandContext(ctx, "git", "cat-file", "--batch")
	command.Dir = repoDir
	command.Env = append(os.Environ(), "GIT_DIR="+repoDir)
	stdin, err := command.StdinPipe()
	if err != nil {
		return 0, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return 0, err
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return 0, err
	}

	writeDone := make(chan error, 1)
	go func() {
		writer := bufio.NewWriter(stdin)
		for _, oid := range oids {
			if _, err := fmt.Fprintf(writer, "%s\n", hex.EncodeToString(oid)); err != nil {
				writeDone <- err
				return
			}
		}
		if err := writer.Flush(); err != nil {
			writeDone <- err
			return
		}
		writeDone <- stdin.Close()
	}()

	reader := bufio.NewReaderSize(stdout, 1<<20)
	var refCount uint32
	for index := range oids {
		line, err := reader.ReadString('\n')
		if err != nil {
			return 0, fmt.Errorf("read cat-file header: %w", err)
		}
		parts := strings.Fields(line)
		if len(parts) != 3 || parts[0] != hex.EncodeToString(oids[index]) {
			return 0, errors.New("unexpected cat-file response")
		}
		size, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || size < 0 {
			return 0, errors.New("invalid cat-file object size")
		}
		var payload []byte
		if parts[1] == "blob" {
			if copied, err := io.CopyN(io.Discard, reader, size); err != nil || copied != size {
				return 0, fmt.Errorf("discard cat-file blob: %w", err)
			}
		} else {
			const maximumMetadataObjectBytes = int64(512 * 1024 * 1024)
			if size > maximumMetadataObjectBytes {
				return 0, errors.New("metadata object exceeds processor capacity")
			}
			payload = make([]byte, size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return 0, fmt.Errorf("read cat-file payload: %w", err)
			}
		}
		if delimiter, err := reader.ReadByte(); err != nil || delimiter != '\n' {
			return 0, errors.New("invalid cat-file payload delimiter")
		}
		typeCode, refs, err := logicalReferences(parts[1], payload)
		if err != nil {
			return 0, err
		}
		typeCodes[index] = typeCode
		refStarts[index] = refCount
		for _, ref := range refs {
			if _, err := refsFile.Write(ref); err != nil {
				return 0, err
			}
			refCount++
		}
	}
	refStarts[len(oids)] = refCount
	if err := <-writeDone; err != nil {
		return 0, err
	}
	if err := command.Wait(); err != nil {
		return 0, fmt.Errorf("git cat-file failed: %w: %s", err, boundedText(stderr.Bytes()))
	}
	return refCount, nil
}

func logicalReferences(objectType string, payload []byte) (byte, [][]byte, error) {
	switch objectType {
	case "commit":
		return 1, parseCommitReferences(payload), nil
	case "tree":
		refs, err := parseTreeReferences(payload)
		return 2, refs, err
	case "blob":
		return 3, nil, nil
	case "tag":
		return 4, parseTagReferences(payload), nil
	default:
		return 0, nil, fmt.Errorf("unsupported object type %q", objectType)
	}
}

func parseCommitReferences(payload []byte) [][]byte {
	var refs [][]byte
	for _, line := range bytes.Split(payload, []byte{'\n'}) {
		if len(line) == 0 {
			break
		}
		for _, prefix := range [][]byte{[]byte("tree "), []byte("parent ")} {
			if bytes.HasPrefix(line, prefix) {
				if oid, ok := decodeObjectID(line[len(prefix):]); ok {
					refs = append(refs, oid)
				}
			}
		}
	}
	return refs
}

func parseTagReferences(payload []byte) [][]byte {
	var target []byte
	hasType := false
	for _, line := range bytes.Split(payload, []byte{'\n'}) {
		if len(line) == 0 {
			break
		}
		if bytes.HasPrefix(line, []byte("object ")) {
			target, _ = decodeObjectID(line[len("object "):])
		}
		if bytes.HasPrefix(line, []byte("type ")) {
			value := string(line[len("type "):])
			hasType = value == "commit" || value == "tree" || value == "blob" || value == "tag"
		}
	}
	if target == nil || !hasType {
		return nil
	}
	return [][]byte{target}
}

func parseTreeReferences(payload []byte) ([][]byte, error) {
	var refs [][]byte
	for cursor := 0; cursor < len(payload); {
		space := bytes.IndexByte(payload[cursor:], ' ')
		if space < 0 {
			return nil, errors.New("invalid tree mode")
		}
		mode := string(payload[cursor : cursor+space])
		cursor += space + 1
		nul := bytes.IndexByte(payload[cursor:], 0)
		if nul < 0 {
			return nil, errors.New("invalid tree name")
		}
		cursor += nul + 1
		if cursor+objectIDBytes > len(payload) {
			return nil, errors.New("truncated tree object id")
		}
		if mode != "160000" {
			refs = append(refs, append([]byte(nil), payload[cursor:cursor+objectIDBytes]...))
		}
		cursor += objectIDBytes
	}
	return refs, nil
}

func decodeObjectID(value []byte) ([]byte, bool) {
	if len(value) < 40 {
		return nil, false
	}
	decoded := make([]byte, objectIDBytes)
	if _, err := hex.Decode(decoded, value[:40]); err != nil {
		return nil, false
	}
	return decoded, true
}

func writePackRefHeader(writer io.Writer, objectCount uint32, packBytes uint64, packChecksum []byte, idxChecksum []byte) error {
	if len(packChecksum) != objectIDBytes || len(idxChecksum) != objectIDBytes {
		return errors.New("invalid checksum size")
	}
	header := make([]byte, packRefHeaderBytes)
	binary.BigEndian.PutUint32(header[0:4], packRefMagic)
	binary.BigEndian.PutUint32(header[4:8], packRefVersion)
	binary.BigEndian.PutUint32(header[8:12], objectCount)
	binary.BigEndian.PutUint64(header[12:20], packBytes)
	copy(header[20:40], packChecksum)
	copy(header[40:60], idxChecksum)
	_, err := writer.Write(header)
	return err
}
