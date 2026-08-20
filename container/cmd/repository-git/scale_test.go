package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const scaleFileBytes = 1_000_000

type fileBridge struct {
	root    string
	mu      sync.Mutex
	objects map[string]string
	errors  []string
}

type directFileBridgeClient struct {
	bridge *fileBridge
}

func copyExact(source string, destination string, expectedBytes int64) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(output, io.LimitReader(input, expectedBytes+1))
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if written != expectedBytes {
		return fmt.Errorf("object size mismatch: expected %d, got %d", expectedBytes, written)
	}
	return nil
}

func (client directFileBridgeClient) download(_ context.Context, key string, destination string, expectedBytes int64) error {
	source, found := client.bridge.path(key)
	if !found {
		return fmt.Errorf("missing object %s", key)
	}
	return copyExact(source, destination, expectedBytes)
}

func (client directFileBridgeClient) upload(_ context.Context, key string, source string, size int64) error {
	destination := client.bridge.outputPath(key)
	if err := copyExact(source, destination, size); err != nil {
		return err
	}
	client.bridge.register(key, destination)
	return nil
}

func (bridge *fileBridge) recordError(message string) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	bridge.errors = append(bridge.errors, message)
}

func (bridge *fileBridge) errorSummary() string {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	return strings.Join(bridge.errors, "; ")
}

type scalePack struct {
	packPath  string
	idxPath   string
	packBytes int64
	idxBytes  int64
}

type scaleResult struct {
	BaseLogicalBytes  int64                       `json:"baseLogicalBytes"`
	FinalLogicalBytes int64                       `json:"finalLogicalBytes"`
	ChurnCommits      int                         `json:"churnCommits"`
	FinalCommit       string                      `json:"finalCommit"`
	Stages            map[string]scaleStageResult `json:"stages"`
	FinalPackCount    int                         `json:"finalPackCount"`
}

type scaleStageResult struct {
	InputPackBytes  int64  `json:"inputPackBytes"`
	ActivePackBytes int64  `json:"activePackBytes"`
	OutputPackBytes int64  `json:"outputPackBytes"`
	OutputIdxBytes  int64  `json:"outputIdxBytes"`
	OutputRefsBytes int64  `json:"outputRefsBytes"`
	ObjectCount     uint32 `json:"objectCount"`
	ScratchBytes    int64  `json:"scratchBytes"`
	ElapsedMS       int64  `json:"elapsedMs"`
}

func (bridge *fileBridge) register(key string, path string) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	bridge.objects[key] = path
}

func (bridge *fileBridge) path(key string) (string, bool) {
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	path, found := bridge.objects[key]
	return path, found
}

func (bridge *fileBridge) outputPath(key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(bridge.root, hex.EncodeToString(digest[:]))
}

func (bridge *fileBridge) handler(writer http.ResponseWriter, request *http.Request) {
	encoded := strings.TrimPrefix(request.URL.Path, "/r2/")
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		http.Error(writer, "invalid key", http.StatusBadRequest)
		return
	}
	key := string(decoded)
	if request.Method == http.MethodGet {
		path, found := bridge.path(key)
		if !found {
			http.NotFound(writer, request)
			return
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			http.Error(writer, "missing object", http.StatusNotFound)
			return
		}
		defer file.Close()
		info, statErr := file.Stat()
		if statErr != nil {
			http.Error(writer, "stat failed", http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		copied, copyErr := io.Copy(writer, file)
		if copyErr != nil {
			bridge.recordError(fmt.Sprintf("GET %s copied=%d error=%v", key, copied, copyErr))
			return
		}
		return
	}
	if request.Method != http.MethodPut || request.ContentLength <= 0 {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := bridge.outputPath(key)
	file, createErr := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if createErr != nil {
		http.Error(writer, "create failed", http.StatusInternalServerError)
		return
	}
	written, copyErr := io.Copy(file, io.LimitReader(request.Body, request.ContentLength+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || written != request.ContentLength {
		_ = os.Remove(path)
		http.Error(writer, "write failed", http.StatusBadRequest)
		return
	}
	bridge.register(key, path)
	writer.WriteHeader(http.StatusNoContent)
}

func requiredScaleInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return parsed
}

func ensureScaleDisk(t *testing.T, path string, minimumBytes uint64) {
	t.Helper()
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		t.Fatal(err)
	}
	available := uint64(stats.Bavail) * uint64(stats.Bsize)
	if available < minimumBytes {
		t.Fatalf("scale test needs %d free bytes; only %d available", minimumBytes, available)
	}
}

func writeDeterministicFile(t *testing.T, path string, seed uint64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64*1024)
	remaining := scaleFileBytes
	state := seed | 1
	for remaining > 0 {
		count := len(buffer)
		if count > remaining {
			count = remaining
		}
		for index := 0; index < count; index++ {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			buffer[index] = byte(state)
		}
		if _, err := file.Write(buffer[:count]); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		remaining -= count
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func generateBinaryRange(t *testing.T, repository string, first int, count int) {
	t.Helper()
	for index := first; index < first+count; index++ {
		path := filepath.Join(
			repository,
			fmt.Sprintf("assets/%02d/deep/%02d/binary-%05d.dat", index%37, index%19, index),
		)
		writeDeterministicFile(t, path, uint64(index+1)*0x9e3779b97f4a7c15)
	}
}

func gitToFile(t *testing.T, repository string, destination string, input string, args ...string) {
	t.Helper()
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", args...)
	command.Dir = repository
	command.Stdin = strings.NewReader(input)
	command.Stdout = file
	command.Stderr = io.Discard
	runErr := command.Run()
	closeErr := file.Close()
	if runErr != nil || closeErr != nil {
		t.Fatalf("git %s failed: run=%v close=%v", args[0], runErr, closeErr)
	}
}

func runScaleStage(t *testing.T, args struct {
	ctx       context.Context
	bridge    *fileBridge
	client    objectBridge
	name      string
	inputPath string
	oldOID    string
	newOID    string
	active    []scalePack
}) (scalePack, scaleStageResult) {
	t.Helper()
	inputInfo, err := os.Stat(args.inputPath)
	if err != nil {
		t.Fatal(err)
	}
	inputKey := args.name + "-input.pack"
	args.bridge.register(inputKey, args.inputPath)
	activeInputs := make([]packInput, 0, len(args.active))
	var activePackBytes int64
	for index, active := range args.active {
		packInfo, packErr := os.Stat(active.packPath)
		idxInfo, idxErr := os.Stat(active.idxPath)
		if packErr != nil || idxErr != nil {
			t.Fatalf("active artifact stat failed: pack=%v idx=%v", packErr, idxErr)
		}
		if packInfo.Size() != active.packBytes || idxInfo.Size() != active.idxBytes {
			t.Fatalf(
				"active artifact size drift: pack expected=%d actual=%d idx expected=%d actual=%d",
				active.packBytes,
				packInfo.Size(),
				active.idxBytes,
				idxInfo.Size(),
			)
		}
		packKey := fmt.Sprintf("%s-active-%02d.pack", args.name, index)
		idxKey := strings.TrimSuffix(packKey, ".pack") + ".idx"
		args.bridge.register(packKey, active.packPath)
		args.bridge.register(idxKey, active.idxPath)
		activeInputs = append(activeInputs, packInput{
			PackKey: packKey, PackBytes: active.packBytes, IdxBytes: active.idxBytes,
		})
		activePackBytes += active.packBytes + active.idxBytes
	}
	outputPackKey := args.name + "-output.pack"
	request := processRequest{
		OperationID:   args.name,
		InputPackKey:  inputKey,
		InputBytes:    inputInfo.Size(),
		ActivePacks:   activeInputs,
		Commands:      []receiveCommand{{OldOID: args.oldOID, NewOID: args.newOID, Ref: "refs/heads/main"}},
		OutputPackKey: outputPackKey,
		OutputIdxKey:  strings.TrimSuffix(outputPackKey, ".pack") + ".idx",
		OutputRefsKey: strings.TrimSuffix(outputPackKey, ".pack") + ".refs",
	}
	started := time.Now()
	result, err := processPackWithBridge(args.ctx, request, args.client)
	if err != nil {
		t.Fatalf("%v; bridge=%s", err, args.bridge.errorSummary())
	}
	packPath, packFound := args.bridge.path(request.OutputPackKey)
	idxPath, idxFound := args.bridge.path(request.OutputIdxKey)
	if !packFound || !idxFound {
		t.Fatal("native processor did not publish pack and index")
	}
	elapsedMS := time.Since(started).Milliseconds()
	return scalePack{
			packPath: packPath, idxPath: idxPath, packBytes: result.PackBytes, idxBytes: result.IdxBytes,
		}, scaleStageResult{
			InputPackBytes: inputInfo.Size(), ActivePackBytes: activePackBytes,
			OutputPackBytes: result.PackBytes, OutputIdxBytes: result.IdxBytes,
			OutputRefsBytes: result.RefsBytes, ObjectCount: result.ObjectCount,
			ScratchBytes: result.ScratchBytes, ElapsedMS: elapsedMS,
		}
}

func TestRepresentativeRepositoryScaleAndAgentChurn(t *testing.T) {
	if os.Getenv("NATIVE_GIT_SCALE") != "1" {
		t.Skip("set NATIVE_GIT_SCALE=1 to run the bounded 1-3 GB native Git test")
	}
	baseFiles := requiredScaleInt(t, "NATIVE_GIT_BASE_FILES", 1000)
	growthFiles := requiredScaleInt(t, "NATIVE_GIT_GROWTH_FILES", 2000)
	churnCommits := requiredScaleInt(t, "NATIVE_GIT_CHURN_COMMITS", 200)
	ensureScaleDisk(t, os.TempDir(), 30_000_000_000)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	repository := t.TempDir()
	gitOutput(t, repository, "init", "-q")
	for index := 0; index < 1000; index++ {
		path := filepath.Join(repository, fmt.Sprintf("docs/section-%03d/page-%04d.md", index%100, index))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(fmt.Sprintf("# Page %d\n\nInitial content.\n", index)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	generateBinaryRange(t, repository, 0, baseFiles)
	gitOutput(t, repository, "add", ".")
	gitOutput(t, repository, "commit", "-q", "-m", "one-gigabyte representative base")
	baseOID := strings.TrimSpace(string(gitOutput(t, repository, "rev-parse", "HEAD")))

	bridge := &fileBridge{root: t.TempDir(), objects: make(map[string]string)}
	client := directFileBridgeClient{bridge: bridge}
	stageMetrics := make(map[string]scaleStageResult)
	baseInput := filepath.Join(t.TempDir(), "base.pack")
	gitToFile(t, repository, baseInput, "", "pack-objects", "--stdout", "--all")
	var basePack scalePack
	basePack, stageMetrics["base"] = runScaleStage(t, struct {
		ctx       context.Context
		bridge    *fileBridge
		client    objectBridge
		name      string
		inputPath string
		oldOID    string
		newOID    string
		active    []scalePack
	}{ctx, bridge, client, "base", baseInput, strings.Repeat("0", 40), baseOID, nil})

	firstChurn := churnCommits / 2
	for index := 0; index < firstChurn; index++ {
		path := filepath.Join(repository, fmt.Sprintf("docs/section-%03d/page-%04d.md", index%100, index%1000))
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := fmt.Fprintf(file, "Agent checkpoint %d: deterministic edit.\n", index)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("write churn: %v %v", writeErr, closeErr)
		}
		gitOutput(t, repository, "add", path)
		gitOutput(t, repository, "commit", "-q", "-m", fmt.Sprintf("agent checkpoint %d", index))
	}
	churnOID := strings.TrimSpace(string(gitOutput(t, repository, "rev-parse", "HEAD")))
	churnInput := filepath.Join(t.TempDir(), "churn.pack")
	gitToFile(t, repository, churnInput, churnOID+"\n^"+baseOID+"\n", "pack-objects", "--stdout", "--revs", "--thin")
	var churnPack scalePack
	churnPack, stageMetrics["churn-at-1gb"] = runScaleStage(t, struct {
		ctx       context.Context
		bridge    *fileBridge
		client    objectBridge
		name      string
		inputPath string
		oldOID    string
		newOID    string
		active    []scalePack
	}{ctx, bridge, client, "churn-one", churnInput, baseOID, churnOID, []scalePack{basePack}})

	generateBinaryRange(t, repository, baseFiles, growthFiles)
	gitOutput(t, repository, "add", ".")
	gitOutput(t, repository, "commit", "-q", "-m", "grow representative repository to three gigabytes")
	growthOID := strings.TrimSpace(string(gitOutput(t, repository, "rev-parse", "HEAD")))
	growthInput := filepath.Join(t.TempDir(), "growth.pack")
	gitToFile(t, repository, growthInput, growthOID+"\n^"+churnOID+"\n", "pack-objects", "--stdout", "--revs", "--thin")
	var growthPack scalePack
	growthPack, stageMetrics["grow-to-3gb"] = runScaleStage(t, struct {
		ctx       context.Context
		bridge    *fileBridge
		client    objectBridge
		name      string
		inputPath string
		oldOID    string
		newOID    string
		active    []scalePack
	}{ctx, bridge, client, "growth", growthInput, churnOID, growthOID, []scalePack{basePack, churnPack}})

	for index := firstChurn; index < churnCommits; index++ {
		path := filepath.Join(repository, fmt.Sprintf("docs/section-%03d/page-%04d.md", index%100, index%1000))
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := fmt.Fprintf(file, "Agent checkpoint %d: deterministic edit.\n", index)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("write churn: %v %v", writeErr, closeErr)
		}
		gitOutput(t, repository, "add", path)
		gitOutput(t, repository, "commit", "-q", "-m", fmt.Sprintf("agent checkpoint %d", index))
	}
	finalOID := strings.TrimSpace(string(gitOutput(t, repository, "rev-parse", "HEAD")))
	finalInput := filepath.Join(t.TempDir(), "final-churn.pack")
	gitToFile(t, repository, finalInput, finalOID+"\n^"+growthOID+"\n", "pack-objects", "--stdout", "--revs", "--thin")
	var finalPack scalePack
	finalPack, stageMetrics["churn-at-3gb"] = runScaleStage(t, struct {
		ctx       context.Context
		bridge    *fileBridge
		client    objectBridge
		name      string
		inputPath string
		oldOID    string
		newOID    string
		active    []scalePack
	}{ctx, bridge, client, "churn-three", finalInput, growthOID, finalOID, []scalePack{basePack, churnPack, growthPack}})

	allPacks := []scalePack{basePack, churnPack, growthPack, finalPack}
	restored := t.TempDir()
	gitOutput(t, restored, "init", "--bare", "-q")
	packDir := filepath.Join(restored, "objects", "pack")
	for index, pack := range allPacks {
		packName := filepath.Join(packDir, fmt.Sprintf("pack-stage-%02d.pack", index))
		idxName := strings.TrimSuffix(packName, ".pack") + ".idx"
		copyFile(t, pack.packPath, packName)
		copyFile(t, pack.idxPath, idxName)
	}
	gitOutput(t, restored, "update-ref", "refs/heads/main", finalOID)
	gitOutput(t, restored, "fsck", "--full", "--no-dangling")
	gitOutput(t, restored, "cat-file", "-e", baseOID+"^{commit}")
	gitOutput(t, restored, "cat-file", "-e", finalOID+"^{commit}")

	result := scaleResult{
		BaseLogicalBytes:  int64(baseFiles) * scaleFileBytes,
		FinalLogicalBytes: int64(baseFiles+growthFiles) * scaleFileBytes,
		ChurnCommits:      churnCommits,
		FinalCommit:       finalOID,
		Stages:            stageMetrics,
		FinalPackCount:    len(allPacks),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("NATIVE_GIT_SCALE_RESULT=%s\n", encoded)
}

func copyFile(t *testing.T, source string, destination string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy failed: %v %v", copyErr, closeErr)
	}
}
