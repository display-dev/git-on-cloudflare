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
	oldContents := bytes.Repeat([]byte("stable-delta-source-line\n"), 16_384)
	if err := os.WriteFile(filepath.Join(source, "file.txt"), oldContents, 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, source, nil, "git", "add", "file.txt")
	runTestCommand(t, source, nil, "git", "commit", "-m", "old")
	oldOID := strings.TrimSpace(string(runTestCommand(t, source, nil, "git", "rev-parse", "HEAD")))
	oldBlobOID := strings.TrimSpace(string(runTestCommand(t, source, nil, "git", "rev-parse", oldOID+":file.txt")))
	newContents := append(append([]byte(nil), oldContents...), []byte("new-tail\n")...)
	if err := os.WriteFile(filepath.Join(source, "file.txt"), newContents, 0o600); err != nil {
		t.Fatal(err)
	}
	runTestCommand(t, source, nil, "git", "add", "file.txt")
	runTestCommand(t, source, nil, "git", "commit", "-m", "new")
	newOID := strings.TrimSpace(string(runTestCommand(t, source, nil, "git", "rev-parse", "HEAD")))

	prerequisite := runTestCommand(t, source, []byte(oldOID+"\n"), "git", "pack-objects", "--stdout", "--revs")
	incoming := runTestCommand(t, source, []byte(newOID+"\n^"+oldOID+"\n"), "git", "pack-objects", "--stdout", "--revs", "--thin")
	thinProbe := filepath.Join(root, "thin-probe.git")
	runTestCommand(t, "", nil, "git", "init", "--bare", thinProbe)
	thinProbeCommand := exec.Command("git", "index-pack", "--stdin", "--fix-thin")
	thinProbeCommand.Dir = thinProbe
	thinProbeCommand.Stdin = bytes.NewReader(incoming)
	if output, err := thinProbeCommand.CombinedOutput(); err == nil {
		t.Fatalf("fixture pack did not require its external thin base: %s", output)
	}
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
		ThinDeltaBaseOIDs:       []string{oldBlobOID},
		RequiredRootOIDs:        []string{oldBlobOID, oldOID},
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
	physicalBlobNode := physicalNode
	physicalBlobNode.Offset = 13
	physicalBlobNode.End = 14
	physicalBlobNode.OID = oldBlobOID
	physicalBlobNode.ObjectType = "blob"
	physicalBlobNode.SemanticRootOIDs = []string{oldBlobOID}
	physicalBlobNode.EntryID = stockPhysicalEntryID(physicalBlobNode)
	manifest.PhysicalNodes = []stockPhysicalNode{physicalNode, physicalBlobNode}
	manifest.TopologicalEntryIDs = []string{physicalNode.EntryID, physicalBlobNode.EntryID}
	manifest.SelectedPackChecksums = []string{physicalNode.PackChecksum}
	manifest.Ranges = []stockRequiredRange{
		{
			EntryID: physicalNode.EntryID, PackChecksum: physicalNode.PackChecksum,
			Start: physicalNode.Offset, End: physicalNode.End, Reason: "required-object",
			RequiredOID: oldOID, SemanticRootOIDs: []string{oldOID},
		},
		{
			EntryID: physicalBlobNode.EntryID, PackChecksum: physicalBlobNode.PackChecksum,
			Start: physicalBlobNode.Offset, End: physicalBlobNode.End, Reason: "required-object",
			RequiredOID: oldBlobOID, SemanticRootOIDs: []string{oldBlobOID},
		},
	}
	manifest.Prerequisite.Key = "prerequisite.pack"
	manifest.Prerequisite.Bytes = int64(len(prerequisite))
	manifest.Prerequisite.SHA256 = hex.EncodeToString(prerequisiteDigest[:])
	manifest.Prerequisite.ObjectOIDs = []string{oldBlobOID, oldOID}
	sort.Strings(manifest.RequiredRootOIDs)
	sort.Strings(manifest.Prerequisite.ObjectOIDs)
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

	// A client may redundantly send a thin pack whose objects and decoding base
	// are already authoritative. Native Git validates the command and hook but
	// emits no replacement pack.
	knownPrerequisite := runTestCommand(t, source, []byte(newOID+"\n"), "git", "pack-objects", "--stdout", "--revs")
	knownPrerequisiteDigest := sha256.Sum256(knownPrerequisite)
	knownRef := "refs/heads/known"
	knownCommandLine := zero + " " + newOID + " " + knownRef + "\x00 report-status\n"
	knownPrefix := append([]byte(pktLength(knownCommandLine)), []byte(knownCommandLine)...)
	knownPrefix = append(knownPrefix, []byte("0000")...)
	knownBody := append(append([]byte(nil), knownPrefix...), incoming...)
	knownBodyDigest := sha256.Sum256(knownBody)
	knownCommands := []receiveCommand{{OldOID: zero, NewOID: newOID, Ref: knownRef}}
	knownManifest := manifest
	knownManifest.OperationID = "known-object-stock-receive"
	knownManifest.Commands = knownCommands
	knownManifest.AdvertisedRefs = []stockAdvertisedRef{{Name: "refs/heads/main", OID: newOID}}
	knownManifest.AdvertisedReachableOIDs = strings.Fields(string(runTestCommand(t, source, nil, "git", "rev-list", "--objects", "--no-object-names", newOID)))
	sort.Strings(knownManifest.AdvertisedReachableOIDs)
	knownManifest.SemanticExternalOIDs = []string{newOID}
	knownManifest.ThinDeltaBaseOIDs = []string{oldBlobOID}
	knownManifest.RequiredRootOIDs = []string{newOID, oldBlobOID}
	sort.Strings(knownManifest.RequiredRootOIDs)
	knownManifest.Closure = stockClosureCounts{IncomingObjectCount: 3}
	knownManifest.Input.Bytes = int64(len(knownBody))
	knownManifest.Input.SHA256 = hex.EncodeToString(knownBodyDigest[:])
	knownManifest.Input.PackOffset = int64(len(knownPrefix))
	knownManifest.Input.PackBytes = int64(len(incoming))
	knownNode := physicalNode
	knownNode.OID = newOID
	knownNode.SemanticRootOIDs = []string{newOID}
	knownNode.EntryID = stockPhysicalEntryID(knownNode)
	knownBaseNode := physicalNode
	knownBaseNode.Offset = 13
	knownBaseNode.End = 14
	knownBaseNode.OID = oldBlobOID
	knownBaseNode.ObjectType = "blob"
	knownBaseNode.SemanticRootOIDs = []string{oldBlobOID}
	knownBaseNode.EntryID = stockPhysicalEntryID(knownBaseNode)
	knownManifest.PhysicalNodes = []stockPhysicalNode{knownNode, knownBaseNode}
	knownManifest.TopologicalEntryIDs = []string{knownNode.EntryID, knownBaseNode.EntryID}
	knownManifest.Ranges = []stockRequiredRange{
		{
			EntryID: knownNode.EntryID, PackChecksum: knownNode.PackChecksum,
			Start: knownNode.Offset, End: knownNode.End, Reason: "required-object",
			RequiredOID: newOID, SemanticRootOIDs: []string{newOID},
		},
		{
			EntryID: knownBaseNode.EntryID, PackChecksum: knownBaseNode.PackChecksum,
			Start: knownBaseNode.Offset, End: knownBaseNode.End, Reason: "required-object",
			RequiredOID: oldBlobOID, SemanticRootOIDs: []string{oldBlobOID},
		},
	}
	knownManifest.Prerequisite.Bytes = int64(len(knownPrerequisite))
	knownManifest.Prerequisite.SHA256 = hex.EncodeToString(knownPrerequisiteDigest[:])
	knownManifest.Prerequisite.ObjectOIDs = []string{oldBlobOID, newOID}
	sort.Strings(knownManifest.Prerequisite.ObjectOIDs)
	knownManifestBytes, err := json.Marshal(knownManifest)
	if err != nil {
		t.Fatal(err)
	}
	knownManifestDigest := sha256.Sum256(knownManifestBytes)
	knownBridge := &stockMemoryBridge{objects: map[string][]byte{
		"input.request": knownBody, "prerequisite.pack": knownPrerequisite, "closure.json": knownManifestBytes,
	}}
	knownRequest := request
	knownRequest.OperationID = knownManifest.OperationID
	knownRequest.InputRequestBytes = int64(len(knownBody))
	knownRequest.InputRequestSHA256 = hex.EncodeToString(knownBodyDigest[:])
	knownRequest.PackOffset = int64(len(knownPrefix))
	knownRequest.PrerequisitePackBytes = int64(len(knownPrerequisite))
	knownRequest.PrerequisitePackSHA256 = hex.EncodeToString(knownPrerequisiteDigest[:])
	knownRequest.ClosureManifestBytes = int64(len(knownManifestBytes))
	knownRequest.ClosureManifestSHA256 = hex.EncodeToString(knownManifestDigest[:])
	knownRequest.AdvertisedRefs = knownManifest.AdvertisedRefs
	knownRequest.Commands = knownCommands
	knownRequest.OutputPackKey = "known-output.pack"
	knownRequest.OutputIdxKey = "known-output.idx"
	knownRequest.OutputRefsKey = "known-output.refs"
	knownResult, err := processStockReceive(context.Background(), knownRequest, knownBridge)
	if err != nil {
		t.Fatal(err)
	}
	if knownResult.ResultKind != "ref-only" || knownResult.ObjectCount != 0 || knownResult.InputPackObjectCount != 3 {
		t.Fatalf("unexpected known-object result %#v", knownResult)
	}
	for _, key := range []string{knownRequest.OutputPackKey, knownRequest.OutputIdxKey, knownRequest.OutputRefsKey} {
		if _, found := knownBridge.objects[key]; found {
			t.Fatalf("ref-only receive fabricated %s", key)
		}
	}

	emptyPack := runTestCommand(t, source, nil, "git", "pack-objects", "--stdout")
	rollbackLine := newOID + " " + oldOID + " refs/heads/main\x00 report-status\n"
	rollbackPrefix := append([]byte(pktLength(rollbackLine)), []byte(rollbackLine)...)
	rollbackPrefix = append(rollbackPrefix, []byte("0000")...)
	rollbackBody := append(append([]byte(nil), rollbackPrefix...), emptyPack...)
	rollbackBodyDigest := sha256.Sum256(rollbackBody)
	rollbackCommands := []receiveCommand{{OldOID: newOID, NewOID: oldOID, Ref: "refs/heads/main"}}
	rollbackManifest := knownManifest
	rollbackManifest.OperationID = "exact-rollback-stock-receive"
	rollbackManifest.Commands = rollbackCommands
	rollbackManifest.SemanticExternalOIDs = []string{oldOID}
	rollbackManifest.ThinDeltaBaseOIDs = []string{}
	rollbackManifest.RequiredRootOIDs = []string{oldOID, newOID}
	sort.Strings(rollbackManifest.RequiredRootOIDs)
	rollbackManifest.Closure = stockClosureCounts{IncomingObjectCount: 0}
	rollbackManifest.Input.Bytes = int64(len(rollbackBody))
	rollbackManifest.Input.SHA256 = hex.EncodeToString(rollbackBodyDigest[:])
	rollbackManifest.Input.PackOffset = int64(len(rollbackPrefix))
	rollbackManifest.Input.PackBytes = int64(len(emptyPack))
	rollbackNodes := make([]stockPhysicalNode, 0, 2)
	for index, oid := range rollbackManifest.RequiredRootOIDs {
		node := physicalNode
		node.Offset = int64(12 + index)
		node.End = node.Offset + 1
		node.OID = oid
		node.SemanticRootOIDs = []string{oid}
		node.EntryID = stockPhysicalEntryID(node)
		rollbackNodes = append(rollbackNodes, node)
	}
	rollbackManifest.PhysicalNodes = rollbackNodes
	rollbackManifest.TopologicalEntryIDs = []string{rollbackNodes[0].EntryID, rollbackNodes[1].EntryID}
	rollbackManifest.Ranges = make([]stockRequiredRange, 0, 2)
	for _, node := range rollbackNodes {
		rollbackManifest.Ranges = append(rollbackManifest.Ranges, stockRequiredRange{
			EntryID: node.EntryID, PackChecksum: node.PackChecksum, Start: node.Offset, End: node.End,
			Reason: "required-object", RequiredOID: node.OID, SemanticRootOIDs: node.SemanticRootOIDs,
		})
	}
	rollbackManifest.Prerequisite.ObjectOIDs = rollbackManifest.RequiredRootOIDs
	rollbackManifestBytes, err := json.Marshal(rollbackManifest)
	if err != nil {
		t.Fatal(err)
	}
	rollbackManifestDigest := sha256.Sum256(rollbackManifestBytes)
	rollbackBridge := &stockMemoryBridge{objects: map[string][]byte{
		"input.request": rollbackBody, "prerequisite.pack": knownPrerequisite, "closure.json": rollbackManifestBytes,
	}}
	rollbackRequest := knownRequest
	rollbackRequest.OperationID = rollbackManifest.OperationID
	rollbackRequest.InputRequestBytes = int64(len(rollbackBody))
	rollbackRequest.InputRequestSHA256 = hex.EncodeToString(rollbackBodyDigest[:])
	rollbackRequest.PackOffset = int64(len(rollbackPrefix))
	rollbackRequest.ClosureManifestBytes = int64(len(rollbackManifestBytes))
	rollbackRequest.ClosureManifestSHA256 = hex.EncodeToString(rollbackManifestDigest[:])
	rollbackRequest.Commands = rollbackCommands
	rollbackRequest.OutputPackKey = "rollback-output.pack"
	rollbackRequest.OutputIdxKey = "rollback-output.idx"
	rollbackRequest.OutputRefsKey = "rollback-output.refs"
	rollbackResult, err := processStockReceive(context.Background(), rollbackRequest, rollbackBridge)
	if err != nil {
		t.Fatal(err)
	}
	if rollbackResult.ResultKind != "ref-only" || rollbackResult.InputPackObjectCount != 0 || rollbackResult.ObjectCount != 0 {
		t.Fatalf("unexpected exact rollback result %#v", rollbackResult)
	}
	rollbackResultJSON, err := json.Marshal(rollbackResult)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rollbackResultJSON, []byte(`"incomingOids":[]`)) {
		t.Fatalf("ref-only closure proof must encode the empty incoming OID set as an array: %s", rollbackResultJSON)
	}
	response, err = decodeStockResponse(rollbackResult.ReceivePackResponse)
	if err != nil || !bytes.Contains(response, []byte("ok refs/heads/main")) {
		t.Fatalf("unexpected exact rollback response %q: %v", response, err)
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

func TestStockReceiveResultKindWireCompatibility(t *testing.T) {
	artifactJSON, err := json.Marshal(stockReceiveResponse{OperationID: "artifact"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(artifactJSON, []byte(`"resultKind"`)) {
		t.Fatalf("ordinary artifact result changed the legacy wire shape: %s", artifactJSON)
	}
	refOnlyJSON, err := json.Marshal(stockReceiveResponse{ResultKind: "ref-only", OperationID: "ref-only"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(refOnlyJSON, []byte(`"resultKind":"ref-only"`)) {
		t.Fatalf("ref-only result omitted its explicit discriminator: %s", refOnlyJSON)
	}
	if got := normalizedStockResultKind(""); got != "artifacts" {
		t.Fatalf("legacy omitted result kind normalized to %q", got)
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
