package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type stockMemoryBridge struct {
	objects map[string][]byte
}

func (bridge *stockMemoryBridge) download(_ context.Context, key string, destination string, expectedBytes int64) error {
	value, found := bridge.objects[key]
	if !found || int64(len(value)) != expectedBytes {
		return os.ErrNotExist
	}
	return os.WriteFile(destination, value, 0o600)
}

func (bridge *stockMemoryBridge) upload(_ context.Context, key string, source string, size int64) error {
	value, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if int64(len(value)) != size {
		return os.ErrInvalid
	}
	bridge.objects[key] = append([]byte(nil), value...)
	return nil
}

func TestStockReceiveUsesLiveQuarantineAndReturnsActualResponse(t *testing.T) {
	gitVersion := strings.TrimSpace(string(runTestCommand(t, "", nil, "git", "version")))
	if gitVersion != "git version 2.50.1 (Apple Git-155)" {
		t.Skipf("semantic gate requires frozen host Git, got %q", gitVersion)
	}

	root := t.TempDir()
	source := filepath.Join(root, "source")
	runTestCommand(t, "", nil, "git", "init", source)
	runTestCommand(t, source, nil, "git", "config", "user.name", "Selective Receive Test")
	runTestCommand(t, source, nil, "git", "config", "user.email", "selective@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "file.txt"), []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, source, nil, "git", "add", "file.txt")
	runTestCommand(t, source, nil, "git", "commit", "-m", "old")
	oldOID := strings.TrimSpace(string(runTestCommand(t, source, nil, "git", "rev-parse", "HEAD")))
	if err := os.WriteFile(filepath.Join(source, "file.txt"), []byte("old\nnew\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, source, nil, "git", "add", "file.txt")
	runTestCommand(t, source, nil, "git", "commit", "-m", "new")
	newOID := strings.TrimSpace(string(runTestCommand(t, source, nil, "git", "rev-parse", "HEAD")))

	prerequisite := runTestCommand(t, source, []byte(oldOID+"\n"), "git", "pack-objects", "--stdout", "--revs")
	incoming := runTestCommand(t, source, []byte(newOID+"\n^"+oldOID+"\n"), "git", "pack-objects", "--stdout", "--revs", "--thin")
	zero := strings.Repeat("0", 40)
	featureRef := "refs/display-prerequisite-000/feature"
	commandLine := zero + " " + newOID + " " + featureRef + "\x00 report-status\n"
	prefix := append([]byte(pktLength(commandLine)), []byte(commandLine)...)
	prefix = append(prefix, []byte("0000")...)
	body := append(append([]byte(nil), prefix...), incoming...)
	bodyDigest := sha256.Sum256(body)
	prerequisiteDigest := sha256.Sum256(prerequisite)
	reachableLines := strings.Fields(string(runTestCommand(t, source, nil, "git", "rev-list", "--objects", "--no-object-names", oldOID)))
	sort.Strings(reachableLines)
	commands := []receiveCommand{{OldOID: zero, NewOID: newOID, Ref: featureRef}}
	advertisedRefs := []stockAdvertisedRef{{Name: "refs/heads/main", OID: oldOID}}
	manifest := stockClosureManifest{
		SchemaVersion:           2,
		OperationID:             "tiny-stock-receive",
		Commands:                commands,
		AdvertisedRefs:          advertisedRefs,
		AdvertisedReachableOIDs: reachableLines,
		SemanticExternalOIDs:    []string{oldOID},
		RequiredRootOIDs:        []string{oldOID},
		Closure: stockClosureCounts{
			IncomingObjectCount: 3, VisitedIncomingObjectCount: 3,
			LogicalEdgeCount: 3, InternalEdgeCount: 2, ExternalEdgeCount: 1,
			ObjectTypeCounts: stockObjectTypeCounts{Commit: 1, Tree: 1, Blob: 1},
		},
	}
	manifest.Input.Key = "input.request"
	manifest.Input.Bytes = int64(len(body))
	manifest.Input.SHA256 = hex.EncodeToString(bodyDigest[:])
	manifest.Input.PackOffset = int64(len(prefix))
	manifest.Input.PackBytes = int64(len(incoming))
	idxDigest := sha256.Sum256([]byte("fixture-idx"))
	prefDigest := sha256.Sum256([]byte("fixture-pref"))
	physicalNode := stockPhysicalNode{
		PackChecksum: hex.EncodeToString(prerequisite[len(prerequisite)-20:]),
		IdxSHA256:    hex.EncodeToString(idxDigest[:]), PrefSHA256: hex.EncodeToString(prefDigest[:]),
		Offset: 12, End: 13, OID: oldOID, ObjectType: "commit", Encoding: "full",
		SemanticRootOIDs: []string{oldOID}, OIDVerified: true, IntegrityBound: true,
	}
	physicalNode.EntryID = stockPhysicalEntryID(physicalNode)
	manifest.PhysicalNodes = []stockPhysicalNode{physicalNode}
	manifest.TopologicalEntryIDs = []string{physicalNode.EntryID}
	manifest.SelectedPackChecksums = []string{physicalNode.PackChecksum}
	manifest.Ranges = []stockRequiredRange{{
		EntryID: physicalNode.EntryID, PackChecksum: physicalNode.PackChecksum,
		Start: physicalNode.Offset, End: physicalNode.End, Reason: "required-object",
		RequiredOID: oldOID, SemanticRootOIDs: []string{oldOID},
	}}
	manifest.Prerequisite.Key = "prerequisite.pack"
	manifest.Prerequisite.Bytes = int64(len(prerequisite))
	manifest.Prerequisite.SHA256 = hex.EncodeToString(prerequisiteDigest[:])
	manifest.Prerequisite.ObjectOIDs = []string{oldOID}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)

	hookExecutable := filepath.Join(root, "repository-git")
	runTestCommand(t, "", nil, "go", "build", "-o", hookExecutable, ".")
	bridge := &stockMemoryBridge{objects: map[string][]byte{
		"input.request":     body,
		"prerequisite.pack": prerequisite,
		"closure.json":      manifestBytes,
	}}
	request := stockReceiveRequest{
		OperationID:     "tiny-stock-receive",
		InputRequestKey: "input.request", InputRequestBytes: int64(len(body)), InputRequestSHA256: hex.EncodeToString(bodyDigest[:]),
		PackOffset: int64(len(prefix)), PrerequisitePackKey: "prerequisite.pack", PrerequisitePackBytes: int64(len(prerequisite)),
		PrerequisitePackSHA256: hex.EncodeToString(prerequisiteDigest[:]),
		ClosureManifestKey:     "closure.json", ClosureManifestBytes: int64(len(manifestBytes)), ClosureManifestSHA256: hex.EncodeToString(manifestDigest[:]),
		AdvertisedRefs: advertisedRefs,
		Commands:       commands,
		OutputPackKey:  "output.pack", OutputIdxKey: "output.idx", OutputRefsKey: "output.refs", hookExecutable: hookExecutable,
	}
	overlappingRoots := manifest
	overlappingRoots.ThinDeltaBaseOIDs = []string{oldOID}
	if err := validateStockClosureManifest(overlappingRoots, request); err == nil || !strings.Contains(err.Error(), "semantic and thin roots overlap") {
		t.Fatalf("expected overlapping semantic/thin roots to fail closed, got %v", err)
	}
	if err := validateStockReceiveRequest(request); err != nil {
		t.Fatal(err)
	}
	result, err := processStockReceive(context.Background(), request, bridge)
	if err != nil {
		t.Fatal(err)
	}
	response, err := decodeStockResponse(result.ReceivePackResponse)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(response, []byte("unpack ok")) || !bytes.Contains(response, []byte("ok "+featureRef)) {
		t.Fatalf("unexpected receive-pack response %q", response)
	}
	if len(result.Trace) != 8 || result.Trace[1].Event != "pre_receive_started" || result.Trace[2].Event != "pre_receive_quarantine_nonempty" || result.Trace[7].Event != "disposable_ref_update_observed" {
		t.Fatalf("unexpected trace %#v", result.Trace)
	}
	if result.InputPackObjectCount != uint32(manifest.Closure.IncomingObjectCount) || result.ObjectCount != result.InputPackObjectCount+uint32(len(manifest.ThinDeltaBaseOIDs)) {
		t.Fatalf("unexpected input/output object counts %d/%d", result.InputPackObjectCount, result.ObjectCount)
	}
	if !result.FreshWorkDirectory || result.RepositoryPackBytesBeforeHydration != 0 || !result.SharedObjectCacheDisabled || result.SkipConnectivityCheck {
		t.Fatalf("unexpected cold runtime proof %#v", result)
	}
	for _, key := range []string{"output.pack", "output.idx", "output.refs"} {
		if len(bridge.objects[key]) == 0 {
			t.Fatalf("missing %s", key)
		}
	}

	reconstructed := filepath.Join(root, "reconstructed.git")
	runTestCommand(t, "", nil, "git", "init", "--bare", reconstructed)
	runTestCommand(t, reconstructed, prerequisite, "git", "index-pack", "--stdin", "--fix-thin")
	runTestCommand(t, reconstructed, bridge.objects["output.pack"], "git", "index-pack", "--stdin", "--fix-thin")
	runTestCommand(t, reconstructed, nil, "git", "update-ref", featureRef, newOID)
	runTestCommand(t, reconstructed, nil, "git", "fsck", "--full", "--strict")
}

func TestStockClosureManifestAcceptsFirstPushWithoutPrerequisiteRoots(t *testing.T) {
	zero := strings.Repeat("0", 40)
	newOID := strings.Repeat("1", 40)
	commands := []receiveCommand{{OldOID: zero, NewOID: newOID, Ref: "refs/heads/main"}}
	request := stockReceiveRequest{
		OperationID: "initial-stock-receive", InputRequestKey: "input.request",
		InputRequestBytes: 100, InputRequestSHA256: strings.Repeat("a", 64), PackOffset: 20,
		PrerequisitePackKey: "prerequisite.pack", PrerequisitePackBytes: 32,
		PrerequisitePackSHA256: strings.Repeat("b", 64), ClosureManifestKey: "closure.json",
		ClosureManifestBytes: 200, ClosureManifestSHA256: strings.Repeat("c", 64),
		Commands: commands, OutputPackKey: "output.pack", OutputIdxKey: "output.idx",
		OutputRefsKey: "output.refs",
	}
	manifest := stockClosureManifest{
		SchemaVersion: 2, OperationID: request.OperationID, Commands: commands,
		Closure: stockClosureCounts{
			IncomingObjectCount: 2, VisitedIncomingObjectCount: 2, LogicalEdgeCount: 1,
			InternalEdgeCount: 1, ObjectTypeCounts: stockObjectTypeCounts{Commit: 1, Tree: 1},
		},
	}
	manifest.Input.Key = request.InputRequestKey
	manifest.Input.Bytes = request.InputRequestBytes
	manifest.Input.SHA256 = request.InputRequestSHA256
	manifest.Input.PackOffset = request.PackOffset
	manifest.Input.PackBytes = request.InputRequestBytes - request.PackOffset
	manifest.Prerequisite.Key = request.PrerequisitePackKey
	manifest.Prerequisite.Bytes = request.PrerequisitePackBytes
	manifest.Prerequisite.SHA256 = request.PrerequisitePackSHA256

	if err := validateStockReceiveRequest(request); err != nil {
		t.Fatalf("first-push request rejected: %v", err)
	}
	if err := validateStockClosureManifest(manifest, request); err != nil {
		t.Fatalf("first-push closure rejected: %v", err)
	}
	manifest.SelectedPackChecksums = []string{strings.Repeat("d", 40)}
	if err := validateStockClosureManifest(manifest, request); err == nil || !strings.Contains(err.Error(), "empty physical graph") {
		t.Fatalf("mixed empty physical graph accepted: %v", err)
	}
}

func TestStockReceiveAcceptsFirstPushWithoutPrerequisiteRoots(t *testing.T) {
	gitVersion := strings.TrimSpace(string(runTestCommand(t, "", nil, "git", "version")))
	if gitVersion != "git version 2.50.1 (Apple Git-155)" {
		t.Skipf("semantic gate requires frozen host Git, got %q", gitVersion)
	}

	root := t.TempDir()
	source := filepath.Join(root, "source")
	runTestCommand(t, "", nil, "git", "init", source)
	runTestCommand(t, source, nil, "git", "config", "user.name", "Selective Receive Test")
	runTestCommand(t, source, nil, "git", "config", "user.email", "selective@example.invalid")
	runTestCommand(t, source, nil, "git", "commit", "--allow-empty", "-m", "initial")
	newOID := strings.TrimSpace(string(runTestCommand(t, source, nil, "git", "rev-parse", "HEAD")))
	incoming := runTestCommand(t, source, nil, "git", "pack-objects", "--stdout", "--all")
	prerequisite := runTestCommand(t, source, nil, "git", "pack-objects", "--stdout", "--revs")
	zero := strings.Repeat("0", 40)
	commandLine := zero + " " + newOID + " refs/heads/main\x00 report-status\n"
	prefix := append([]byte(pktLength(commandLine)), []byte(commandLine)...)
	prefix = append(prefix, []byte("0000")...)
	body := append(append([]byte(nil), prefix...), incoming...)
	bodyDigest := sha256.Sum256(body)
	prerequisiteDigest := sha256.Sum256(prerequisite)
	commands := []receiveCommand{{OldOID: zero, NewOID: newOID, Ref: "refs/heads/main"}}
	manifest := stockClosureManifest{
		SchemaVersion: 2, OperationID: "initial-stock-receive", Commands: commands,
		AdvertisedRefs: []stockAdvertisedRef{}, AdvertisedReachableOIDs: []string{},
		SemanticExternalOIDs: []string{}, ThinDeltaBaseOIDs: []string{},
		RequiredRootOIDs: []string{}, PhysicalNodes: []stockPhysicalNode{},
		Dependencies: []stockPhysicalDependency{}, TopologicalEntryIDs: []string{},
		SelectedPackChecksums: []string{}, Ranges: []stockRequiredRange{},
		Closure: stockClosureCounts{
			IncomingObjectCount: 2, VisitedIncomingObjectCount: 2, LogicalEdgeCount: 1,
			InternalEdgeCount: 1, ObjectTypeCounts: stockObjectTypeCounts{Commit: 1, Tree: 1},
		},
	}
	manifest.Input.Key = "input.request"
	manifest.Input.Bytes = int64(len(body))
	manifest.Input.SHA256 = hex.EncodeToString(bodyDigest[:])
	manifest.Input.PackOffset = int64(len(prefix))
	manifest.Input.PackBytes = int64(len(incoming))
	manifest.Prerequisite.Key = "prerequisite.pack"
	manifest.Prerequisite.Bytes = int64(len(prerequisite))
	manifest.Prerequisite.SHA256 = hex.EncodeToString(prerequisiteDigest[:])
	manifest.Prerequisite.ObjectOIDs = []string{}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)

	hookExecutable := filepath.Join(root, "repository-git")
	runTestCommand(t, "", nil, "go", "build", "-o", hookExecutable, ".")
	bridge := &stockMemoryBridge{objects: map[string][]byte{
		"input.request": body, "prerequisite.pack": prerequisite, "closure.json": manifestBytes,
	}}
	request := stockReceiveRequest{
		OperationID: "initial-stock-receive", InputRequestKey: "input.request",
		InputRequestBytes: int64(len(body)), InputRequestSHA256: hex.EncodeToString(bodyDigest[:]),
		PackOffset: int64(len(prefix)), PrerequisitePackKey: "prerequisite.pack",
		PrerequisitePackBytes: int64(len(prerequisite)), PrerequisitePackSHA256: hex.EncodeToString(prerequisiteDigest[:]),
		ClosureManifestKey: "closure.json", ClosureManifestBytes: int64(len(manifestBytes)), ClosureManifestSHA256: hex.EncodeToString(manifestDigest[:]),
		AdvertisedRefs: []stockAdvertisedRef{}, Commands: commands,
		OutputPackKey: "output.pack", OutputIdxKey: "output.idx", OutputRefsKey: "output.refs",
		hookExecutable: hookExecutable,
	}
	result, err := processStockReceive(context.Background(), request, bridge)
	if err != nil {
		t.Fatal(err)
	}
	response, err := decodeStockResponse(result.ReceivePackResponse)
	if err != nil || !bytes.Contains(response, []byte("unpack ok")) || !bytes.Contains(response, []byte("ok refs/heads/main")) {
		t.Fatalf("unexpected first-push response %q: %v", response, err)
	}
}

func pktLength(line string) string {
	const digits = "0123456789abcdef"
	length := len(line) + 4
	return string([]byte{digits[(length>>12)&15], digits[(length>>8)&15], digits[(length>>4)&15], digits[length&15]})
}

func runTestCommand(t *testing.T, directory string, stdin []byte, name string, args ...string) []byte {
	t.Helper()
	command := exec.Command(name, args...)
	if directory != "" {
		command.Dir = directory
	}
	command.Stdin = bytes.NewReader(stdin)
	command.Env = append(os.Environ(), "GIT_AUTHOR_DATE=2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T00:00:00Z")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v: %s", name, err, output)
	}
	return output
}

func decodeStockResponse(value string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(value)
}
