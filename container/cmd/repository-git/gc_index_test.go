package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaintenanceUploadRejectsPermanentBridgeFailures(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 413, 408, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { writer.WriteHeader(status) }))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "artifact")
			if err := os.WriteFile(path, []byte("immutable"), 0600); err != nil {
				t.Fatal(err)
			}
			bridge := bridgeClient{client: server.Client(), baseURL: server.URL + "/"}
			err := bridge.upload(context.Background(), "output", path, 9)
			if err == nil {
				t.Fatal("rejection accepted")
			}
			var transient transientProcessError
			classified := outputUploadFailure(processRequest{Maintenance: &gcIndexRequest{}}, "upload", err)
			if errors.As(classified, &transient) != (status == 408 || status == 429 || status >= 500) {
				t.Fatal("incorrect maintenance retry classification")
			}
			if !errors.As(outputUploadFailure(processRequest{}, "upload", err), &transient) {
				t.Fatal("receive retry behavior changed")
			}
		})
	}
}

func TestMaintenanceIndexesAndPublishesOnlyExactClosure(t *testing.T) {
	repo := t.TempDir()
	gitOutput(t, repo, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "retained.txt"), []byte("retained content\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, repo, "add", ".")
	gitOutput(t, repo, "commit", "-m", "retained root")
	root := strings.TrimSpace(string(gitOutput(t, repo, "rev-parse", "HEAD")))
	pack := gitInput(t, repo, root+"\n", "pack-objects", "--stdout", "--revs", "--window=0")
	packPath := filepath.Join(t.TempDir(), "input.pack")
	if err := os.WriteFile(packPath, pack, 0600); err != nil {
		t.Fatal(err)
	}
	gitOutput(t, repo, "index-pack", packPath)
	idx, err := os.ReadFile(strings.TrimSuffix(packPath, ".pack") + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	oids, checksum, _, err := parseIndex(idx)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.New()
	for _, oid := range oids {
		fmt.Fprintf(digest, "%s\n", hex.EncodeToString(oid))
	}
	input := processRequest{
		OperationID: "gc-index", InputPackKey: "input.pack", InputBytes: int64(len(pack)),
		OutputPackKey: "output.pack", OutputIdxKey: "output.idx", OutputRefsKey: "output.refs",
		Maintenance: &gcIndexRequest{Roots: []string{root}, ObjectCount: uint32(len(oids)), ObjectSetSHA256: hex.EncodeToString(digest.Sum(nil)), PackSHA1: hex.EncodeToString(checksum), ResultKey: "result.json"},
	}
	if err := validateProcessRequest(input); err != nil {
		t.Fatal(err)
	}
	bridge := memoryBridge{objects: map[string][]byte{"input.pack": pack}, gets: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(bridge.handler))
	defer server.Close()
	client := bridgeClient{client: server.Client(), baseURL: server.URL + "/r2/"}
	result, err := processPackWithBridge(context.Background(), input, client)
	if err != nil {
		t.Fatal(err)
	}
	if result.Maintenance == nil || result.Maintenance.ObjectSetSHA256 != input.Maintenance.ObjectSetSHA256 || result.CacheHitBytes != 0 || bridge.gets["input.pack"] != 1 {
		t.Fatalf("unexpected maintenance proof: %+v", result)
	}
	if !bytes.Equal(bridge.objects["output.pack"], pack) {
		t.Fatal("native maintenance changed the pack")
	}
	var receipt processResponse
	if err := json.Unmarshal(bridge.objects["result.json"], &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.PackSHA1 != result.PackSHA1 || receipt.Maintenance.ObjectSetSHA256 != result.Maintenance.ObjectSetSHA256 {
		t.Fatal("durable receipt did not bind output")
	}
	if got := strings.TrimSpace(string(gitOutput(t, repo, "rev-parse", "HEAD"))); got != root {
		t.Fatal("maintenance changed source refs")
	}
	for _, mutation := range []struct {
		name   string
		change func(*gcIndexRequest)
	}{
		{"wrong object set", func(gc *gcIndexRequest) { gc.ObjectSetSHA256 = strings.Repeat("0", 64) }},
		{"wrong pack", func(gc *gcIndexRequest) { gc.PackSHA1 = strings.Repeat("0", 40) }},
		{"missing root", func(gc *gcIndexRequest) { gc.Roots = []string{strings.Repeat("f", 40)} }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			gc := *input.Maintenance
			mutation.change(&gc)
			rejected := input
			rejected.Maintenance = &gc
			bridge.objects = map[string][]byte{"input.pack": pack}
			if _, err := processPackWithBridge(context.Background(), rejected, client); err == nil {
				t.Fatal("invalid closure accepted")
			}
			if len(bridge.objects) != 1 {
				t.Fatal("invalid closure wrote output")
			}
		})
	}
	input.Commands = []receiveCommand{{OldOID: strings.Repeat("0", 40), NewOID: root, Ref: "refs/heads/main"}}
	if validateProcessRequest(input) == nil {
		t.Fatal("maintenance accepted receive commands")
	}
}
