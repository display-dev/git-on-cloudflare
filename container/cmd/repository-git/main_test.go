package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestBridgeFailureIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "temporary bridge outage", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := bridgeClient{client: server.Client(), baseURL: server.URL + "/r2/"}
	request := processRequest{
		OperationID:   "bridge-outage",
		InputPackKey:  "input.pack",
		InputBytes:    1,
		Commands:      []receiveCommand{{OldOID: strings.Repeat("0", 40), NewOID: strings.Repeat("1", 40), Ref: "refs/heads/main"}},
		OutputPackKey: "output.pack",
		OutputIdxKey:  "output.idx",
		OutputRefsKey: "output.refs",
	}
	_, err := processPackWithBridge(context.Background(), request, client)
	var transient transientProcessError
	if !errors.As(err, &transient) {
		t.Fatalf("bridge failure was not retryable: %v", err)
	}
	recorder := httptest.NewRecorder()
	writeProcessError(recorder, err)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected retryable 503, got %d", recorder.Code)
	}
}

func TestProcessHandlerRejectsConcurrentOperationAsRetryable(t *testing.T) {
	if !acquireProcessSlot() {
		t.Fatal("processor slot unexpectedly occupied")
	}
	defer releaseProcessSlot()
	requestBody, err := json.Marshal(processRequest{
		OperationID:   "concurrent-operation",
		InputPackKey:  "input.pack",
		InputBytes:    1,
		Commands:      []receiveCommand{{OldOID: strings.Repeat("0", 40), NewOID: strings.Repeat("1", 40), Ref: "refs/heads/main"}},
		OutputPackKey: "output.pack",
		OutputIdxKey:  "output.idx",
		OutputRefsKey: "output.refs",
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	processHandler(recorder, httptest.NewRequest(http.MethodPost, "/process", bytes.NewReader(requestBody)))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected concurrent processing to return 503, got %d", recorder.Code)
	}
}

func TestProcessErrorCategoryDoesNotExposeGitErrorDetails(t *testing.T) {
	privateDetail := "fatal: object deadbeefdeadbeefdeadbeefdeadbeefdeadbeef is missing"
	if category := processErrorCategory(errors.New(privateDetail)); category != "rejected" {
		t.Fatalf("unexpected category %q", category)
	}
	if strings.Contains(processErrorCategory(errors.New(privateDetail)), "deadbeef") {
		t.Fatal("error category exposed repository detail")
	}
}

type memoryBridge struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    map[string]int
}

func (bridge *memoryBridge) handler(writer http.ResponseWriter, request *http.Request) {
	encoded := strings.TrimPrefix(request.URL.Path, "/r2/")
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		http.Error(writer, "invalid key", http.StatusBadRequest)
		return
	}
	key := string(decoded)
	bridge.mu.Lock()
	defer bridge.mu.Unlock()
	switch request.Method {
	case http.MethodGet:
		value, found := bridge.objects[key]
		if !found {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Length", strconv.Itoa(len(value)))
		if bridge.gets != nil {
			bridge.gets[key]++
		}
		_, _ = writer.Write(value)
	case http.MethodPut:
		value, readErr := io.ReadAll(request.Body)
		if readErr != nil || int64(len(value)) != request.ContentLength {
			http.Error(writer, "invalid body", http.StatusBadRequest)
			return
		}
		bridge.objects[key] = value
		writer.WriteHeader(http.StatusNoContent)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func gitOutput(t *testing.T, directory string, args ...string) []byte {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Display Test",
		"GIT_AUTHOR_EMAIL=test@example.invalid",
		"GIT_COMMITTER_NAME=Display Test",
		"GIT_COMMITTER_EMAIL=test@example.invalid",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v: %s", args[0], err, output)
	}
	return output
}

func gitInput(t *testing.T, directory string, input string, args ...string) []byte {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s failed: %v: %s", args[0], err, output)
	}
	return output
}

func TestValidateProcessRequestAcceptsGeneralGitRefs(t *testing.T) {
	request := processRequest{
		OperationID:   "general-ref",
		InputPackKey:  "input.pack",
		InputBytes:    1,
		Commands:      []receiveCommand{{OldOID: strings.Repeat("0", 40), NewOID: strings.Repeat("1", 40), Ref: "refs/notes/display"}},
		OutputPackKey: "output.pack",
		OutputIdxKey:  "output.idx",
		OutputRefsKey: "output.refs",
	}
	if err := validateProcessRequest(request); err != nil {
		t.Fatalf("valid general ref rejected: %v", err)
	}
	request.Commands[0].Ref = "refs/heads/invalid..name"
	if err := validateProcessRequest(request); err == nil {
		t.Fatal("invalid ref accepted")
	}
}

func TestValidateProcessRequestRejectsOversizedInputWithoutActivePacks(t *testing.T) {
	request := processRequest{
		OperationID:   "oversized-empty-repository",
		InputPackKey:  "input.pack",
		InputBytes:    maxHydratedBytes + 1,
		Commands:      []receiveCommand{{OldOID: strings.Repeat("0", 40), NewOID: strings.Repeat("1", 40), Ref: "refs/heads/main"}},
		OutputPackKey: "output.pack",
		OutputIdxKey:  "output.idx",
		OutputRefsKey: "output.refs",
	}
	if err := validateProcessRequest(request); err == nil {
		t.Fatal("oversized input accepted without active packs")
	}
}

func TestProcessPackProducesReadableGitArtifacts(t *testing.T) {
	source := t.TempDir()
	gitOutput(t, source, "init", "-q")
	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("native git\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, source, "add", "README.md")
	gitOutput(t, source, "commit", "-q", "-m", "initial")
	commitOID := strings.TrimSpace(string(gitOutput(t, source, "rev-parse", "HEAD")))
	pack := gitOutput(t, source, "pack-objects", "--stdout", "--all")

	bridge := &memoryBridge{objects: map[string][]byte{"input.pack": pack}}
	server := httptest.NewServer(http.HandlerFunc(bridge.handler))
	defer server.Close()
	client := bridgeClient{client: server.Client(), baseURL: server.URL + "/r2/"}
	request := processRequest{
		OperationID:   "operation-1",
		InputPackKey:  "input.pack",
		InputBytes:    int64(len(pack)),
		Commands:      []receiveCommand{{OldOID: strings.Repeat("0", 40), NewOID: commitOID, Ref: "refs/heads/main"}},
		OutputPackKey: "output.pack",
		OutputIdxKey:  "output.idx",
		OutputRefsKey: "output.refs",
	}
	if err := validateProcessRequest(request); err != nil {
		t.Fatal(err)
	}
	result, err := processPackWithBridge(context.Background(), request, client)
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectCount < 3 || result.PackBytes != int64(len(bridge.objects["output.pack"])) {
		t.Fatalf("unexpected result: %+v", result)
	}
	refs := bridge.objects["output.refs"]
	if len(refs) < packRefHeaderBytes || binary.BigEndian.Uint32(refs[0:4]) != packRefMagic {
		t.Fatal("invalid PREF sidecar")
	}
	if binary.BigEndian.Uint32(refs[8:12]) != result.ObjectCount {
		t.Fatal("PREF object count does not match result")
	}

	restored := t.TempDir()
	gitOutput(t, restored, "init", "--bare", "-q")
	packDir := filepath.Join(restored, "objects", "pack")
	if err := os.WriteFile(filepath.Join(packDir, "pack-native.pack"), bridge.objects["output.pack"], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-native.idx"), bridge.objects["output.idx"], 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, restored, "cat-file", "-e", commitOID+"^{commit}")
	gitOutput(t, restored, "update-ref", "refs/heads/main", commitOID)
	gitOutput(t, restored, "fsck", "--full", "--no-dangling")
}

func TestProcessPackFixesThinInputAgainstActiveCatalog(t *testing.T) {
	source := t.TempDir()
	gitOutput(t, source, "init", "-q")
	filePath := filepath.Join(source, "content.txt")
	if err := os.WriteFile(filePath, []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, source, "add", "content.txt")
	gitOutput(t, source, "commit", "-q", "-m", "base")
	baseOID := strings.TrimSpace(string(gitOutput(t, source, "rev-parse", "HEAD")))
	basePack := gitInput(t, source, baseOID+"\n", "pack-objects", "--stdout", "--revs")

	if err := os.WriteFile(filePath, []byte("base\nnext\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, source, "commit", "-q", "-am", "next")
	nextOID := strings.TrimSpace(string(gitOutput(t, source, "rev-parse", "HEAD")))
	thinPack := gitInput(
		t,
		source,
		nextOID+"\n^"+baseOID+"\n",
		"pack-objects",
		"--stdout",
		"--revs",
		"--thin",
	)

	indexDir := t.TempDir()
	activePackPath := filepath.Join(indexDir, "active.pack")
	activeIdxPath := filepath.Join(indexDir, "active.idx")
	if err := os.WriteFile(activePackPath, basePack, 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, indexDir, "index-pack", "-o", activeIdxPath, activePackPath)
	activeIdx, err := os.ReadFile(activeIdxPath)
	if err != nil {
		t.Fatal(err)
	}

	bridge := &memoryBridge{objects: map[string][]byte{
		"active.pack": basePack,
		"active.idx":  activeIdx,
		"input.pack":  thinPack,
	}}
	server := httptest.NewServer(http.HandlerFunc(bridge.handler))
	defer server.Close()
	client := bridgeClient{client: server.Client(), baseURL: server.URL + "/r2/"}
	request := processRequest{
		OperationID:  "operation-thin",
		InputPackKey: "input.pack",
		InputBytes:   int64(len(thinPack)),
		ActivePacks: []packInput{{
			PackKey:   "active.pack",
			PackBytes: int64(len(basePack)),
			IdxBytes:  int64(len(activeIdx)),
		}},
		Commands:      []receiveCommand{{OldOID: baseOID, NewOID: nextOID, Ref: "refs/heads/main"}},
		OutputPackKey: "output.pack",
		OutputIdxKey:  "output.idx",
		OutputRefsKey: "output.refs",
	}
	result, err := processPackWithBridge(context.Background(), request, client)
	if err != nil {
		t.Fatal(err)
	}
	if result.ObjectCount < 3 {
		t.Fatalf("expected fixed pack objects, got %+v", result)
	}

	restored := t.TempDir()
	gitOutput(t, restored, "init", "--bare", "-q")
	packDir := filepath.Join(restored, "objects", "pack")
	if err := os.WriteFile(filepath.Join(packDir, "pack-active.pack"), basePack, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-active.idx"), activeIdx, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-native.pack"), bridge.objects["output.pack"], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-native.idx"), bridge.objects["output.idx"], 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, restored, "update-ref", "refs/heads/main", nextOID)
	gitOutput(t, restored, "fsck", "--full", "--no-dangling")
	gitOutput(t, restored, "cat-file", "-e", baseOID+"^{commit}")
}

func TestProcessPackReusesImmutableActivePackCache(t *testing.T) {
	source := t.TempDir()
	gitOutput(t, source, "init", "-q")
	if err := os.WriteFile(filepath.Join(source, "content.txt"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, source, "add", "content.txt")
	gitOutput(t, source, "commit", "-q", "-m", "base")
	baseOID := strings.TrimSpace(string(gitOutput(t, source, "rev-parse", "HEAD")))
	basePack := gitInput(t, source, baseOID+"\n", "pack-objects", "--stdout", "--revs")

	indexDir := t.TempDir()
	activePackPath := filepath.Join(indexDir, "active.pack")
	activeIdxPath := filepath.Join(indexDir, "active.idx")
	if err := os.WriteFile(activePackPath, basePack, 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, indexDir, "index-pack", "-o", activeIdxPath, activePackPath)
	activeIdx, err := os.ReadFile(activeIdxPath)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(source, "content.txt"), []byte("base\nnext\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, source, "commit", "-q", "-am", "next")
	nextOID := strings.TrimSpace(string(gitOutput(t, source, "rev-parse", "HEAD")))
	thinPack := gitInput(t, source, nextOID+"\n^"+baseOID+"\n", "pack-objects", "--stdout", "--revs", "--thin")

	bridge := &memoryBridge{objects: map[string][]byte{
		"active.pack": basePack,
		"active.idx":  activeIdx,
		"input.pack":  thinPack,
	}, gets: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(bridge.handler))
	defer server.Close()
	client := bridgeClient{client: server.Client(), baseURL: server.URL + "/r2/"}
	request := processRequest{
		OperationID:   "cache-first",
		InputPackKey:  "input.pack",
		InputBytes:    int64(len(thinPack)),
		ActivePacks:   []packInput{{PackKey: "active.pack", PackBytes: int64(len(basePack)), IdxBytes: int64(len(activeIdx))}},
		Commands:      []receiveCommand{{OldOID: baseOID, NewOID: nextOID, Ref: "refs/heads/main"}},
		OutputPackKey: "output-first.pack",
		OutputIdxKey:  "output-first.idx",
		OutputRefsKey: "output-first.refs",
	}
	cacheRoot := t.TempDir()
	first, err := processPackWithBridgeAtCache(context.Background(), request, client, cacheRoot)
	if err != nil {
		t.Fatal(err)
	}
	request.OperationID = "cache-second"
	request.OutputPackKey = "output-second.pack"
	request.OutputIdxKey = "output-second.idx"
	request.OutputRefsKey = "output-second.refs"
	second, err := processPackWithBridgeAtCache(context.Background(), request, client, cacheRoot)
	if err != nil {
		t.Fatal(err)
	}
	if bridge.gets["active.pack"] != 1 || bridge.gets["active.idx"] != 1 {
		t.Fatalf("active artifacts were downloaded more than once: %+v", bridge.gets)
	}
	expectedActiveBytes := int64(len(basePack) + len(activeIdx))
	if first.DownloadedBytes != expectedActiveBytes+int64(len(thinPack)) || first.CacheHitBytes != 0 {
		t.Fatalf("unexpected cold hydration metrics: %+v", first)
	}
	if second.DownloadedBytes != int64(len(thinPack)) || second.CacheHitBytes != expectedActiveBytes {
		t.Fatalf("unexpected warm hydration metrics: %+v", second)
	}

	activeCachePath := cachePath(cacheRoot, "active.idx", int64(len(activeIdx)))
	if err := os.WriteFile(activeCachePath, make([]byte, len(activeIdx)), 0o600); err != nil {
		t.Fatal(err)
	}
	request.OperationID = "cache-corrupt"
	request.OutputPackKey = "output-corrupt.pack"
	request.OutputIdxKey = "output-corrupt.idx"
	request.OutputRefsKey = "output-corrupt.refs"
	if _, err := processPackWithBridgeAtCache(context.Background(), request, client, cacheRoot); err == nil {
		t.Fatal("expected same-size corrupt cache entry to fail native validation")
	} else {
		var transient transientProcessError
		if !errors.As(err, &transient) {
			t.Fatalf("corrupt warm cache must trigger a cold retry, got %v", err)
		}
	}
	if _, err := os.Stat(activeCachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt cache entry was not evicted: %v", err)
	}

	request.OperationID = "cache-recovered"
	request.OutputPackKey = "output-recovered.pack"
	request.OutputIdxKey = "output-recovered.idx"
	request.OutputRefsKey = "output-recovered.refs"
	if _, err := processPackWithBridgeAtCache(context.Background(), request, client, cacheRoot); err != nil {
		t.Fatalf("cache did not recover from authoritative bridge bytes: %v", err)
	}
	if bridge.gets["active.pack"] != 2 || bridge.gets["active.idx"] != 2 {
		t.Fatalf("corrupt active artifacts were not re-downloaded exactly once: %+v", bridge.gets)
	}
}
