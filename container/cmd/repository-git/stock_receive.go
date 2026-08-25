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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	maxStockRequestBytes       = int64(16 * 1024 * 1024)
	maxStockResponseBytes      = int64(1024 * 1024)
	maxStockMetadataBytes      = int64(16 * 1024 * 1024)
	maxStockClosureObjects     = 100_000
	maxStockClosureEdges       = 500_000
	maxStockPhysicalNodes      = 256
	maxStockDependencyDepth    = 255
	maxStockMetadataObjectSize = int64(8 * 1024 * 1024)
	maxStockTraceEvents        = 128
)

type stockAdvertisedRef struct {
	Name string `json:"name"`
	OID  string `json:"oid"`
}

type stockReceiveRequest struct {
	OperationID            string               `json:"operationId"`
	InputRequestKey        string               `json:"inputRequestKey"`
	InputRequestBytes      int64                `json:"inputRequestBytes"`
	InputRequestSHA256     string               `json:"inputRequestSha256"`
	PackOffset             int64                `json:"packOffset"`
	PrerequisitePackKey    string               `json:"prerequisitePackKey"`
	PrerequisitePackBytes  int64                `json:"prerequisitePackBytes"`
	PrerequisitePackSHA256 string               `json:"prerequisitePackSha256"`
	ClosureManifestKey     string               `json:"closureManifestKey"`
	ClosureManifestBytes   int64                `json:"closureManifestBytes"`
	ClosureManifestSHA256  string               `json:"closureManifestSha256"`
	AdvertisedRefs         []stockAdvertisedRef `json:"advertisedRefs"`
	Commands               []receiveCommand     `json:"commands"`
	OutputPackKey          string               `json:"outputPackKey"`
	OutputIdxKey           string               `json:"outputIdxKey"`
	OutputRefsKey          string               `json:"outputRefsKey"`
	hookExecutable         string               `json:"-"`
}

type stockRequiredRange struct {
	EntryID          string   `json:"entryId"`
	PackChecksum     string   `json:"packChecksum"`
	Start            int64    `json:"start"`
	End              int64    `json:"end"`
	Reason           string   `json:"reason"`
	RequiredOID      string   `json:"requiredOid"`
	SemanticRootOIDs []string `json:"semanticRootOids"`
}

type stockPhysicalNode struct {
	EntryID          string   `json:"entryId"`
	PackChecksum     string   `json:"packChecksum"`
	IdxSHA256        string   `json:"idxSha256"`
	PrefSHA256       string   `json:"prefSha256"`
	Offset           int64    `json:"offset"`
	End              int64    `json:"end"`
	OID              string   `json:"oid"`
	ObjectType       string   `json:"objectType"`
	Encoding         string   `json:"encoding"`
	SemanticRootOIDs []string `json:"semanticRootOids"`
	OIDVerified      bool     `json:"oidVerified"`
	IntegrityBound   bool     `json:"integrityBound"`
	BaseEntryID      string   `json:"baseEntryId,omitempty"`
	BaseOID          string   `json:"baseOid,omitempty"`
}

type stockPhysicalDependency struct {
	DependentEntryID string `json:"dependentEntryId"`
	BaseEntryID      string `json:"baseEntryId"`
	Kind             string `json:"kind"`
	BaseOffset       *int64 `json:"baseOffset,omitempty"`
	BaseOID          string `json:"baseOid,omitempty"`
}

type stockObjectTypeCounts struct {
	Commit int `json:"commit"`
	Tree   int `json:"tree"`
	Blob   int `json:"blob"`
	Tag    int `json:"tag"`
}

type stockClosureCounts struct {
	IncomingObjectCount        int                   `json:"incomingObjectCount"`
	VisitedIncomingObjectCount int                   `json:"visitedIncomingObjectCount"`
	LogicalEdgeCount           int                   `json:"logicalEdgeCount"`
	InternalEdgeCount          int                   `json:"internalEdgeCount"`
	ExternalEdgeCount          int                   `json:"externalEdgeCount"`
	MissingObjectCount         int                   `json:"missingObjectCount"`
	ObjectTypeCounts           stockObjectTypeCounts `json:"objectTypeCounts"`
}

type stockClosureManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	OperationID   string `json:"operationId"`
	Input         struct {
		Key        string `json:"key"`
		Bytes      int64  `json:"bytes"`
		SHA256     string `json:"sha256"`
		PackOffset int64  `json:"packOffset"`
		PackBytes  int64  `json:"packBytes"`
	} `json:"input"`
	Commands                []receiveCommand          `json:"commands"`
	AdvertisedRefs          []stockAdvertisedRef      `json:"advertisedRefs"`
	AdvertisedReachableOIDs []string                  `json:"advertisedReachableOids"`
	SemanticExternalOIDs    []string                  `json:"semanticExternalOids"`
	ThinDeltaBaseOIDs       []string                  `json:"thinDeltaBaseOids"`
	RequiredRootOIDs        []string                  `json:"requiredRootOids"`
	PhysicalNodes           []stockPhysicalNode       `json:"physicalNodes"`
	Dependencies            []stockPhysicalDependency `json:"dependencies"`
	TopologicalEntryIDs     []string                  `json:"topologicalEntryIds"`
	SelectedPackChecksums   []string                  `json:"selectedPackChecksums"`
	Closure                 stockClosureCounts        `json:"closure"`
	Ranges                  []stockRequiredRange      `json:"ranges"`
	Prerequisite            struct {
		Key        string   `json:"key"`
		Bytes      int64    `json:"bytes"`
		SHA256     string   `json:"sha256"`
		ObjectOIDs []string `json:"objectOids"`
	} `json:"prerequisite"`
}

type stockClosureProof struct {
	PlanSHA256                 string                `json:"planSha256"`
	IncomingOIDs               []string              `json:"incomingOids"`
	SemanticExternalOIDs       []string              `json:"semanticExternalOids"`
	VisitedIncomingObjectCount int                   `json:"visitedIncomingObjectCount"`
	LogicalEdgeCount           int                   `json:"logicalEdgeCount"`
	InternalEdgeCount          int                   `json:"internalEdgeCount"`
	ExternalEdgeCount          int                   `json:"externalEdgeCount"`
	MissingObjectCount         int                   `json:"missingObjectCount"`
	ObjectTypeCounts           stockObjectTypeCounts `json:"objectTypeCounts"`
}

type stockTraceEvent struct {
	Sequence int    `json:"sequence"`
	Event    string `json:"event"`
}

type stockReceiveResponse struct {
	OperationID                        string            `json:"operationId"`
	ReceivePackResponse                string            `json:"receivePackResponse"`
	ReceiveResponseBytes               int64             `json:"receiveResponseBytes"`
	InputRequestSHA256                 string            `json:"inputRequestSha256"`
	PackBytes                          int64             `json:"packBytes"`
	IdxBytes                           int64             `json:"idxBytes"`
	RefsBytes                          int64             `json:"refsBytes"`
	PackSHA1                           string            `json:"packSha1"`
	PackSHA256                         string            `json:"packSha256"`
	IdxSHA256                          string            `json:"idxSha256"`
	RefsSHA256                         string            `json:"refsSha256"`
	ObjectCount                        uint32            `json:"objectCount"`
	InputPackObjectCount               uint32            `json:"inputPackObjectCount"`
	ElapsedMS                          int64             `json:"elapsedMs"`
	Trace                              []stockTraceEvent `json:"trace"`
	QuarantineInsideOwnedRoot          bool              `json:"quarantinePathInsideOwnedWorkRoot"`
	QuarantineRemoved                  bool              `json:"quarantineRemovedAfterReceive"`
	QuarantinePathNonEmpty             bool              `json:"quarantinePathNonEmpty"`
	FreshWorkDirectory                 bool              `json:"freshWorkDirectory"`
	RepositoryPackBytesBeforeHydration int64             `json:"repositoryPackBytesBeforeHydration"`
	SharedObjectCacheDisabled          bool              `json:"sharedObjectCacheDisabled"`
	SkipConnectivityCheck              bool              `json:"skipConnectivityCheck"`
	PlanSHA256                         string            `json:"planSha256"`
	ClosureProof                       stockClosureProof `json:"closureProof"`
}

type stockGitFence struct {
	Home         string
	XDGConfig    string
	GlobalConfig string
	Template     string
}

type stockGitFenceContextKey struct{}

func newStockGitFence(workDir string) (stockGitFence, error) {
	fence := stockGitFence{
		Home:         filepath.Join(workDir, "git-home"),
		XDGConfig:    filepath.Join(workDir, "git-xdg"),
		GlobalConfig: filepath.Join(workDir, "git-global-config"),
		Template:     filepath.Join(workDir, "git-empty-template"),
	}
	for _, directory := range []string{fence.Home, fence.XDGConfig, fence.Template} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return stockGitFence{}, err
		}
	}
	if err := os.WriteFile(fence.GlobalConfig, nil, 0o600); err != nil {
		return stockGitFence{}, err
	}
	return fence, nil
}

func stockGitEnvironment(ctx context.Context, gitDir string, extra ...string) []string {
	fence, fenced := ctx.Value(stockGitFenceContextKey{}).(stockGitFence)
	environment := make([]string, 0, len(os.Environ())+8+len(extra))
	for _, value := range os.Environ() {
		name := value
		if separator := strings.IndexByte(value, '='); separator >= 0 {
			name = value[:separator]
		}
		if fenced && (name == "HOME" || name == "XDG_CONFIG_HOME" || strings.HasPrefix(name, "GIT_")) {
			continue
		}
		environment = append(environment, value)
	}
	if fenced {
		environment = append(environment,
			"HOME="+fence.Home,
			"XDG_CONFIG_HOME="+fence.XDGConfig,
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_CONFIG_GLOBAL="+fence.GlobalConfig,
		)
	}
	if gitDir != "" {
		environment = append(environment, "GIT_DIR="+gitDir)
	}
	return append(environment, extra...)
}

func repositoryPackBodyBytes(repoDir string) (int64, error) {
	entries, err := os.ReadDir(filepath.Join(repoDir, "objects", "pack"))
	if err != nil {
		return 0, err
	}
	if len(entries) > 1024 {
		return 0, errors.New("repository pack entry bound exceeded")
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || total > maxStockMetadataBytes-info.Size() {
			return 0, errors.New("repository pack body measurement failed")
		}
		total += info.Size()
	}
	return total, nil
}

func sharedObjectCacheDisabled(repoDir string) bool {
	for _, name := range []string{"alternates", "http-alternates"} {
		if _, err := os.Lstat(filepath.Join(repoDir, "objects", "info", name)); !errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	return true
}

type stockHookConfig struct {
	AdvertisedOIDs       []string `json:"advertisedOids"`
	TracePath            string   `json:"tracePath"`
	QuarantineRecordPath string   `json:"quarantineRecordPath"`
	ClosureRecordPath    string   `json:"closureRecordPath"`
	PlanSHA256           string   `json:"planSha256"`
}

func stockBridgeBaseURL() (string, error) {
	value := os.Getenv("REPO_R2_BASE_URL")
	if value == "" {
		return bridgeBaseURL, nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasSuffix(parsed.Path, "/") {
		return "", errors.New("invalid stock bridge base url")
	}
	host := parsed.Hostname()
	address := net.ParseIP(host)
	if host != "localhost" && (address == nil || !address.IsLoopback()) {
		return "", errors.New("stock bridge must be loopback")
	}
	return value, nil
}

func runStockReceiveStdio(ctx context.Context, reader io.Reader, writer io.Writer) error {
	decoder := json.NewDecoder(io.LimitReader(reader, maxStockMetadataBytes+1))
	decoder.DisallowUnknownFields()
	var input stockReceiveRequest
	if err := decoder.Decode(&input); err != nil {
		return errors.New("invalid stock receive request")
	}
	if err := validateStockReceiveRequest(input); err != nil {
		return err
	}
	baseURL, err := stockBridgeBaseURL()
	if err != nil {
		return err
	}
	client := bridgeClient{
		client:  &http.Client{Timeout: 15 * time.Minute, Transport: &http.Transport{DisableKeepAlives: true}},
		baseURL: baseURL,
	}
	result, err := processStockReceive(ctx, input, client)
	if err != nil {
		return err
	}
	return json.NewEncoder(writer).Encode(result)
}

func stockReceiveHandler(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var input stockReceiveRequest
	if err := decoder.Decode(&input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid stock receive request")
		return
	}
	if err := validateStockReceiveRequest(input); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !acquireProcessSlot() {
		writeProcessError(writer, transientFailure("repository processor busy", errors.New("another operation is active")))
		return
	}
	defer releaseProcessSlot()

	startedAt := time.Now()
	baseURL, err := stockBridgeBaseURL()
	if err != nil {
		writeProcessError(writer, err)
		return
	}
	client := bridgeClient{
		client:  &http.Client{Timeout: containerHTTPClient, Transport: &http.Transport{DisableKeepAlives: true}},
		baseURL: baseURL,
	}
	result, err := processStockReceive(request.Context(), input, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repository-git: stock receive failed: %s\n", processErrorCategory(err))
		writeProcessError(writer, err)
		return
	}
	result.ElapsedMS = time.Since(startedAt).Milliseconds()
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(writer).Encode(result)
}

func validateStockReceiveRequest(input stockReceiveRequest) error {
	if input.OperationID == "" || len(input.OperationID) > 200 {
		return errors.New("invalid operation id")
	}
	if input.InputRequestKey == "" || input.InputRequestBytes <= 0 || input.InputRequestBytes > maxStockRequestBytes {
		return errors.New("invalid stock receive request body declaration")
	}
	if len(input.InputRequestSHA256) != 64 || input.PackOffset <= 0 || input.PackOffset >= input.InputRequestBytes {
		return errors.New("invalid stock receive request binding")
	}
	if input.PrerequisitePackKey == "" || input.PrerequisitePackBytes <= 0 || input.PrerequisitePackBytes > maxStockMetadataBytes || len(input.PrerequisitePackSHA256) != 64 {
		return errors.New("invalid prerequisite pack declaration")
	}
	if input.ClosureManifestKey == "" || input.ClosureManifestBytes <= 0 || input.ClosureManifestBytes > maxStockMetadataBytes || len(input.ClosureManifestSHA256) != 64 {
		return errors.New("invalid closure manifest declaration")
	}
	if len(input.AdvertisedRefs) > maxCommands || len(input.Commands) == 0 || len(input.Commands) > maxCommands {
		return errors.New("invalid stock receive command or advertisement count")
	}
	for _, ref := range input.AdvertisedRefs {
		if !objectIDPattern.MatchString(ref.OID) || !refNamePattern.MatchString(ref.Name) {
			return errors.New("invalid advertised ref")
		}
	}
	for _, command := range input.Commands {
		if !objectIDPattern.MatchString(command.OldOID) || !objectIDPattern.MatchString(command.NewOID) || !refNamePattern.MatchString(command.Ref) {
			return errors.New("invalid receive command")
		}
	}
	if input.OutputPackKey == "" || input.OutputIdxKey == "" || input.OutputRefsKey == "" {
		return errors.New("missing output key")
	}
	return nil
}

func validateSortedUniqueOIDs(values []string, allowEmpty bool) error {
	if (!allowEmpty && len(values) == 0) || len(values) > maxStockClosureObjects {
		return errors.New("invalid closure oid count")
	}
	prior := ""
	for _, oid := range values {
		if !objectIDPattern.MatchString(oid) || (prior != "" && oid <= prior) {
			return errors.New("invalid sorted closure oid set")
		}
		prior = oid
	}
	return nil
}

func prerequisiteBoundaryRefPrefix(commands []receiveCommand) (string, error) {
	// Git forbids a ref and one of its path descendants from coexisting. There
	// are at most maxCommands command refs, so maxCommands+1 sibling candidates
	// guarantee one prefix without an exact or directory/file collision.
	for candidate := 0; candidate <= len(commands); candidate++ {
		prefix := fmt.Sprintf("refs/display-prerequisite-%03d", candidate)
		collides := false
		for _, command := range commands {
			if command.Ref == prefix || strings.HasPrefix(command.Ref, prefix+"/") || strings.HasPrefix(prefix, command.Ref+"/") {
				collides = true
				break
			}
		}
		if !collides {
			return prefix, nil
		}
	}
	return "", errors.New("prerequisite boundary ref namespace exhausted")
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func stockPhysicalEntryID(node stockPhysicalNode) string {
	bound := strings.Join([]string{
		"stock-physical-entry-v1",
		node.PackChecksum,
		node.IdxSHA256,
		node.PrefSHA256,
		strconv.FormatInt(node.Offset, 10),
		strconv.FormatInt(node.End, 10),
		node.OID,
	}, "\x00")
	digest := sha256.Sum256([]byte(bound))
	return hex.EncodeToString(digest[:])
}

func stringSetEqual(left []string, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for _, value := range left {
		if _, ok := right[value]; !ok {
			return false
		}
	}
	return true
}

func compareStockPhysicalNode(left stockPhysicalNode, right stockPhysicalNode) int {
	if left.PackChecksum < right.PackChecksum {
		return -1
	}
	if left.PackChecksum > right.PackChecksum {
		return 1
	}
	if left.Offset < right.Offset {
		return -1
	}
	if left.Offset > right.Offset {
		return 1
	}
	if left.End < right.End {
		return -1
	}
	if left.End > right.End {
		return 1
	}
	if left.OID < right.OID {
		return -1
	}
	if left.OID > right.OID {
		return 1
	}
	return 0
}

func validateStockPhysicalGraph(manifest stockClosureManifest, requiredSet map[string]struct{}) error {
	if len(requiredSet) == 0 {
		if len(manifest.PhysicalNodes) != 0 || len(manifest.Dependencies) != 0 ||
			len(manifest.TopologicalEntryIDs) != 0 || len(manifest.Ranges) != 0 ||
			len(manifest.SelectedPackChecksums) != 0 {
			return errors.New("closure manifest empty physical graph mismatch")
		}
		return nil
	}
	if len(manifest.PhysicalNodes) == 0 || len(manifest.PhysicalNodes) > maxStockPhysicalNodes ||
		len(manifest.Ranges) != len(manifest.PhysicalNodes) || len(manifest.TopologicalEntryIDs) != len(manifest.PhysicalNodes) {
		return errors.New("closure manifest physical node count mismatch")
	}
	nodes := make(map[string]stockPhysicalNode, len(manifest.PhysicalNodes))
	selected := make(map[string]struct{})
	for index, node := range manifest.PhysicalNodes {
		if !isSHA256Hex(node.EntryID) || node.EntryID != stockPhysicalEntryID(node) ||
			!objectIDPattern.MatchString(node.PackChecksum) || !isSHA256Hex(node.IdxSHA256) || !isSHA256Hex(node.PrefSHA256) ||
			node.Offset < 0 || node.End <= node.Offset || !objectIDPattern.MatchString(node.OID) ||
			!node.OIDVerified || !node.IntegrityBound ||
			(index > 0 && compareStockPhysicalNode(manifest.PhysicalNodes[index-1], node) >= 0) {
			return errors.New("closure manifest physical node is invalid")
		}
		if node.ObjectType != "commit" && node.ObjectType != "tree" && node.ObjectType != "blob" && node.ObjectType != "tag" {
			return errors.New("closure manifest physical object type is invalid")
		}
		if node.Encoding != "full" && node.Encoding != "ofs-delta" && node.Encoding != "ref-delta" {
			return errors.New("closure manifest physical encoding is invalid")
		}
		if err := validateSortedUniqueOIDs(node.SemanticRootOIDs, false); err != nil {
			return err
		}
		for _, oid := range node.SemanticRootOIDs {
			if _, ok := requiredSet[oid]; !ok {
				return errors.New("closure manifest physical attribution is invalid")
			}
		}
		if _, duplicate := nodes[node.EntryID]; duplicate {
			return errors.New("closure manifest duplicate physical node")
		}
		nodes[node.EntryID] = node
		selected[node.PackChecksum] = struct{}{}
	}
	if err := validateSortedUniqueOIDs(manifest.SelectedPackChecksums, false); err != nil || !stringSetEqual(manifest.SelectedPackChecksums, selected) {
		return errors.New("closure manifest selected pack set mismatch")
	}

	dependencies := make(map[string]stockPhysicalDependency, len(manifest.Dependencies))
	canonicalDependencies := append(
		make([]stockPhysicalDependency, 0, len(manifest.Dependencies)),
		manifest.Dependencies...,
	)
	sort.Slice(canonicalDependencies, func(i, j int) bool {
		left, right := canonicalDependencies[i], canonicalDependencies[j]
		if order := compareStockPhysicalNode(nodes[left.DependentEntryID], nodes[right.DependentEntryID]); order != 0 {
			return order < 0
		}
		if order := compareStockPhysicalNode(nodes[left.BaseEntryID], nodes[right.BaseEntryID]); order != 0 {
			return order < 0
		}
		return left.Kind < right.Kind
	})
	if len(canonicalDependencies) != 0 && !reflect.DeepEqual(canonicalDependencies, manifest.Dependencies) {
		return errors.New("closure manifest physical dependencies are not canonical")
	}
	for _, dependency := range manifest.Dependencies {
		dependent, dependentOK := nodes[dependency.DependentEntryID]
		base, baseOK := nodes[dependency.BaseEntryID]
		if !dependentOK || !baseOK || dependency.DependentEntryID == dependency.BaseEntryID {
			return errors.New("closure manifest physical dependency is invalid")
		}
		if _, duplicate := dependencies[dependency.DependentEntryID]; duplicate {
			return errors.New("closure manifest duplicate physical dependency")
		}
		if dependent.BaseEntryID != base.EntryID || dependent.BaseOID != base.OID {
			return errors.New("closure manifest physical base binding mismatch")
		}
		switch dependency.Kind {
		case "ofs":
			if dependent.Encoding != "ofs-delta" || dependency.BaseOffset == nil || *dependency.BaseOffset != base.Offset ||
				base.Offset >= dependent.Offset || dependency.BaseOID != "" {
				return errors.New("closure manifest OFS dependency is invalid")
			}
		case "ref":
			if dependent.Encoding != "ref-delta" || dependency.BaseOffset != nil || dependency.BaseOID != base.OID {
				return errors.New("closure manifest REF dependency is invalid")
			}
		default:
			return errors.New("closure manifest physical dependency kind is invalid")
		}
		dependencies[dependent.EntryID] = dependency
	}
	for _, node := range manifest.PhysicalNodes {
		_, hasDependency := dependencies[node.EntryID]
		if (node.Encoding == "full") != !hasDependency || (node.Encoding == "full" && (node.BaseEntryID != "" || node.BaseOID != "")) {
			return errors.New("closure manifest physical dependency cardinality mismatch")
		}
	}

	topologicalIndex := make(map[string]int, len(manifest.TopologicalEntryIDs))
	for index, entryID := range manifest.TopologicalEntryIDs {
		if _, ok := nodes[entryID]; !ok {
			return errors.New("closure manifest topology references missing node")
		}
		if _, duplicate := topologicalIndex[entryID]; duplicate {
			return errors.New("closure manifest topology duplicates node")
		}
		topologicalIndex[entryID] = index
	}
	for _, dependency := range manifest.Dependencies {
		if topologicalIndex[dependency.BaseEntryID] >= topologicalIndex[dependency.DependentEntryID] {
			return errors.New("closure manifest topology is not base first")
		}
	}
	childrenByBase := make(map[string][]string)
	ready := make([]string, 0, len(nodes))
	for entryID := range nodes {
		if _, dependent := dependencies[entryID]; !dependent {
			ready = append(ready, entryID)
		}
	}
	for _, dependency := range manifest.Dependencies {
		childrenByBase[dependency.BaseEntryID] = append(childrenByBase[dependency.BaseEntryID], dependency.DependentEntryID)
	}
	lessEntryID := func(left string, right string) bool {
		return compareStockPhysicalNode(nodes[left], nodes[right]) < 0
	}
	for base := range childrenByBase {
		sort.Slice(childrenByBase[base], func(i, j int) bool {
			return lessEntryID(childrenByBase[base][i], childrenByBase[base][j])
		})
	}
	sort.Slice(ready, func(i, j int) bool { return lessEntryID(ready[i], ready[j]) })
	canonicalTopology := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		entryID := ready[0]
		ready = ready[1:]
		canonicalTopology = append(canonicalTopology, entryID)
		ready = append(ready, childrenByBase[entryID]...)
		sort.Slice(ready, func(i, j int) bool { return lessEntryID(ready[i], ready[j]) })
	}
	if !reflect.DeepEqual(canonicalTopology, manifest.TopologicalEntryIDs) {
		return errors.New("closure manifest topology is not canonical")
	}
	for entryID := range nodes {
		seen := make(map[string]struct{})
		current := entryID
		depth := 0
		for {
			dependency, ok := dependencies[current]
			if !ok {
				break
			}
			if _, duplicate := seen[current]; duplicate || depth >= maxStockDependencyDepth {
				return errors.New("closure manifest physical dependency cycle or depth limit")
			}
			seen[current] = struct{}{}
			current = dependency.BaseEntryID
			depth++
		}
	}

	for _, node := range manifest.PhysicalNodes {
		expectedRoots := make(map[string]struct{})
		if _, semantic := requiredSet[node.OID]; semantic {
			expectedRoots[node.OID] = struct{}{}
		}
		for _, dependency := range manifest.Dependencies {
			if dependency.BaseEntryID != node.EntryID {
				continue
			}
			for _, oid := range nodes[dependency.DependentEntryID].SemanticRootOIDs {
				expectedRoots[oid] = struct{}{}
			}
		}
		if !stringSetEqual(node.SemanticRootOIDs, expectedRoots) {
			return errors.New("closure manifest physical root attribution mismatch")
		}
	}
	for rootOID := range requiredSet {
		found := false
		for _, node := range manifest.PhysicalNodes {
			if node.OID == rootOID {
				for _, attributed := range node.SemanticRootOIDs {
					if attributed == rootOID {
						found = true
					}
				}
			}
		}
		if !found {
			return errors.New("closure manifest semantic root physical node missing")
		}
	}

	ranges := make(map[string]stockRequiredRange, len(manifest.Ranges))
	for index, value := range manifest.Ranges {
		if value.EntryID != manifest.TopologicalEntryIDs[index] {
			return errors.New("closure manifest physical ranges are not topological")
		}
		node, ok := nodes[value.EntryID]
		if !ok || value.PackChecksum != node.PackChecksum || value.Start != node.Offset || value.End != node.End ||
			value.RequiredOID != node.OID || value.Reason != "required-object" || !reflect.DeepEqual(value.SemanticRootOIDs, node.SemanticRootOIDs) {
			return errors.New("closure manifest physical range is invalid")
		}
		if _, duplicate := ranges[value.EntryID]; duplicate {
			return errors.New("closure manifest duplicate physical range")
		}
		ranges[value.EntryID] = value
	}
	return nil
}

func validateStockClosureManifest(manifest stockClosureManifest, input stockReceiveRequest) error {
	if manifest.SchemaVersion != 2 || manifest.OperationID != input.OperationID {
		return errors.New("closure manifest operation binding mismatch")
	}
	if manifest.Input.Key != input.InputRequestKey || manifest.Input.Bytes != input.InputRequestBytes ||
		manifest.Input.SHA256 != input.InputRequestSHA256 || manifest.Input.PackOffset != input.PackOffset ||
		manifest.Input.PackBytes != input.InputRequestBytes-input.PackOffset {
		return errors.New("closure manifest input binding mismatch")
	}
	if !reflect.DeepEqual(manifest.Commands, input.Commands) || !reflect.DeepEqual(manifest.AdvertisedRefs, input.AdvertisedRefs) {
		return errors.New("closure manifest command binding mismatch")
	}
	if err := validateSortedUniqueOIDs(manifest.AdvertisedReachableOIDs, true); err != nil {
		return err
	}
	if err := validateSortedUniqueOIDs(manifest.SemanticExternalOIDs, true); err != nil {
		return err
	}
	if err := validateSortedUniqueOIDs(manifest.ThinDeltaBaseOIDs, true); err != nil {
		return err
	}
	if err := validateSortedUniqueOIDs(manifest.RequiredRootOIDs, true); err != nil {
		return err
	}
	boundarySet := make(map[string]struct{}, len(manifest.AdvertisedReachableOIDs))
	for _, oid := range manifest.AdvertisedReachableOIDs {
		boundarySet[oid] = struct{}{}
	}
	for _, ref := range manifest.AdvertisedRefs {
		if _, ok := boundarySet[ref.OID]; !ok {
			return errors.New("advertised ref is outside closure boundary")
		}
	}
	requiredSet := make(map[string]struct{}, len(manifest.RequiredRootOIDs))
	for _, oid := range manifest.RequiredRootOIDs {
		requiredSet[oid] = struct{}{}
	}
	semanticSet := make(map[string]struct{}, len(manifest.SemanticExternalOIDs))
	for _, oid := range manifest.SemanticExternalOIDs {
		semanticSet[oid] = struct{}{}
	}
	for _, oid := range manifest.ThinDeltaBaseOIDs {
		if _, overlap := semanticSet[oid]; overlap {
			return errors.New("closure manifest semantic and thin roots overlap")
		}
	}
	union := make(map[string]struct{}, len(manifest.SemanticExternalOIDs)+len(manifest.ThinDeltaBaseOIDs))
	for _, oid := range append(append([]string(nil), manifest.SemanticExternalOIDs...), manifest.ThinDeltaBaseOIDs...) {
		union[oid] = struct{}{}
	}
	if len(union) != len(requiredSet) {
		return errors.New("closure manifest required root union mismatch")
	}
	for oid := range union {
		if _, ok := requiredSet[oid]; !ok {
			return errors.New("closure manifest required root union mismatch")
		}
	}
	for _, command := range input.Commands {
		if !isZeroOID(command.OldOID) {
			if _, ok := requiredSet[command.OldOID]; !ok {
				return errors.New("command old oid is absent from prerequisite roots")
			}
		}
	}
	if err := validateStockPhysicalGraph(manifest, requiredSet); err != nil {
		return err
	}
	counts := manifest.Closure
	if counts.IncomingObjectCount <= 0 || counts.IncomingObjectCount > maxStockClosureObjects ||
		counts.VisitedIncomingObjectCount != counts.IncomingObjectCount ||
		counts.LogicalEdgeCount < 0 || counts.LogicalEdgeCount > maxStockClosureEdges ||
		counts.InternalEdgeCount < 0 || counts.ExternalEdgeCount < 0 ||
		counts.InternalEdgeCount+counts.ExternalEdgeCount != counts.LogicalEdgeCount || counts.MissingObjectCount != 0 {
		return errors.New("closure manifest counts are invalid")
	}
	if counts.ObjectTypeCounts.Commit < 0 || counts.ObjectTypeCounts.Tree < 0 || counts.ObjectTypeCounts.Blob < 0 || counts.ObjectTypeCounts.Tag < 0 ||
		counts.ObjectTypeCounts.Commit+counts.ObjectTypeCounts.Tree+counts.ObjectTypeCounts.Blob+counts.ObjectTypeCounts.Tag != counts.VisitedIncomingObjectCount {
		return errors.New("closure manifest type counts are invalid")
	}
	if manifest.Prerequisite.Key != input.PrerequisitePackKey || manifest.Prerequisite.Bytes != input.PrerequisitePackBytes || manifest.Prerequisite.SHA256 != input.PrerequisitePackSHA256 ||
		!reflect.DeepEqual(manifest.Prerequisite.ObjectOIDs, manifest.RequiredRootOIDs) {
		return errors.New("closure manifest prerequisite binding mismatch")
	}
	return nil
}

func processStockReceive(ctx context.Context, input stockReceiveRequest, client objectBridge) (stockReceiveResponse, error) {
	workDir, err := os.MkdirTemp("", "repository-stock-receive-")
	if err != nil {
		return stockReceiveResponse{}, err
	}
	defer os.RemoveAll(workDir)
	fence, err := newStockGitFence(workDir)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	ctx = context.WithValue(ctx, stockGitFenceContextKey{}, fence)

	requestPath := filepath.Join(workDir, "receive-request.bin")
	if err := client.download(ctx, input.InputRequestKey, requestPath, input.InputRequestBytes); err != nil {
		return stockReceiveResponse{}, transientFailure("download stock receive request", err)
	}
	requestDigest, err := sha256File(requestPath)
	if err != nil || requestDigest != input.InputRequestSHA256 {
		return stockReceiveResponse{}, errors.New("stock receive request digest mismatch")
	}
	requestBytes, err := os.ReadFile(requestPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	if err := validateStockCommandPrefix(requestBytes, input); err != nil {
		return stockReceiveResponse{}, err
	}
	inputPackObjectCount, err := readStockInputPackObjectCount(requestBytes, input.PackOffset)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	manifestPath := filepath.Join(workDir, "closure-manifest.json")
	if err := client.download(ctx, input.ClosureManifestKey, manifestPath, input.ClosureManifestBytes); err != nil {
		return stockReceiveResponse{}, transientFailure("download closure manifest", err)
	}
	manifestDigest, err := sha256File(manifestPath)
	if err != nil || manifestDigest != input.ClosureManifestSHA256 {
		return stockReceiveResponse{}, errors.New("closure manifest digest mismatch")
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil || int64(len(manifestBytes)) != input.ClosureManifestBytes {
		return stockReceiveResponse{}, errors.New("closure manifest size mismatch")
	}
	var manifest stockClosureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return stockReceiveResponse{}, errors.New("closure manifest is invalid")
	}
	if err := validateStockClosureManifest(manifest, input); err != nil {
		return stockReceiveResponse{}, err
	}
	if int(inputPackObjectCount) != manifest.Closure.IncomingObjectCount {
		return stockReceiveResponse{}, errors.New("input pack count did not match closure manifest")
	}

	repoDir := filepath.Join(workDir, "repo.git")
	if _, err := os.Lstat(repoDir); !errors.Is(err, os.ErrNotExist) {
		return stockReceiveResponse{}, errors.New("stock repository work directory was not fresh")
	}
	if err := runGit(ctx, "", "init", "--bare", "--template="+fence.Template, repoDir); err != nil {
		return stockReceiveResponse{}, err
	}
	if err := os.Mkdir(filepath.Join(repoDir, "hooks"), 0o700); err != nil {
		return stockReceiveResponse{}, err
	}
	repositoryPackBytesBeforeHydration, err := repositoryPackBodyBytes(repoDir)
	if err != nil || repositoryPackBytesBeforeHydration != 0 {
		return stockReceiveResponse{}, errors.New("stock repository was not empty before prerequisite hydration")
	}
	sharedCacheDisabled := sharedObjectCacheDisabled(repoDir)
	if !sharedCacheDisabled {
		return stockReceiveResponse{}, errors.New("stock repository inherited a shared object cache")
	}
	if err := runGit(ctx, repoDir, "config", "receive.unpackLimit", "0"); err != nil {
		return stockReceiveResponse{}, err
	}
	if input.PrerequisitePackKey != "" {
		prerequisitePath := filepath.Join(workDir, "prerequisite.pack")
		if err := client.download(ctx, input.PrerequisitePackKey, prerequisitePath, input.PrerequisitePackBytes); err != nil {
			return stockReceiveResponse{}, transientFailure("download prerequisite pack", err)
		}
		digest, digestErr := sha256File(prerequisitePath)
		if digestErr != nil || digest != input.PrerequisitePackSHA256 {
			return stockReceiveResponse{}, errors.New("prerequisite pack digest mismatch")
		}
		if err := indexPrerequisitePack(ctx, repoDir, prerequisitePath); err != nil {
			return stockReceiveResponse{}, err
		}
	}
	// Git's connectivity walk stops at repository refs. Command old-OIDs do not
	// cover a new branch based on an older advertised commit, so bind every
	// selectively hydrated prerequisite root under a disposable private ref.
	boundaryRefPrefix, err := prerequisiteBoundaryRefPrefix(input.Commands)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	for index, oid := range manifest.RequiredRootOIDs {
		ref := fmt.Sprintf("%s/%03d", boundaryRefPrefix, index)
		if err := runGit(ctx, repoDir, "update-ref", ref, oid); err != nil {
			return stockReceiveResponse{}, fmt.Errorf("install prerequisite boundary ref: %w", err)
		}
	}
	for _, command := range input.Commands {
		if isZeroOID(command.OldOID) {
			continue
		}
		if err := runGit(ctx, repoDir, "update-ref", command.Ref, command.OldOID); err != nil {
			return stockReceiveResponse{}, fmt.Errorf("install command ref: %w", err)
		}
	}

	tracePath := filepath.Join(workDir, "trace.jsonl")
	configPath := filepath.Join(workDir, "hook-config.json")
	quarantineRecordPath := filepath.Join(workDir, "quarantine-path")
	closureRecordPath := filepath.Join(workDir, "closure-proof.json")
	configBytes, _ := json.Marshal(stockHookConfig{
		AdvertisedOIDs:       manifest.AdvertisedReachableOIDs,
		TracePath:            tracePath,
		QuarantineRecordPath: quarantineRecordPath,
		ClosureRecordPath:    closureRecordPath,
		PlanSHA256:           input.ClosureManifestSHA256,
	})
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		return stockReceiveResponse{}, err
	}
	executable := input.hookExecutable
	if executable == "" {
		executable, err = os.Executable()
		if err != nil {
			return stockReceiveResponse{}, err
		}
	}
	hookPath := filepath.Join(repoDir, "hooks", "pre-receive")
	hook := "#!/bin/sh\nexec \"$DISPLAY_STOCK_RECEIVE_EXECUTABLE\" stock-pre-receive\n"
	if err := os.WriteFile(hookPath, []byte(hook), 0o700); err != nil {
		return stockReceiveResponse{}, err
	}
	if err := runGit(ctx, repoDir, "config", "core.hooksPath", filepath.Join(repoDir, "hooks")); err != nil {
		return stockReceiveResponse{}, err
	}
	if err := appendStockTrace(tracePath, "receive_pack_invoked"); err != nil {
		return stockReceiveResponse{}, err
	}

	receivePackArgs := []string{
		"-c", "core.hooksPath=" + filepath.Join(repoDir, "hooks"),
		"receive-pack", "--stateless-rpc", repoDir,
	}
	skipConnectivityCheck := false
	command := exec.CommandContext(ctx, "git", receivePackArgs...)
	command.Dir = repoDir
	command.Env = stockGitEnvironment(ctx, repoDir,
		"DISPLAY_STOCK_RECEIVE_EXECUTABLE="+executable,
		"DISPLAY_STOCK_RECEIVE_CONFIG="+configPath,
	)
	command.Stdin = bytes.NewReader(requestBytes)
	responseBuffer := &boundedBuffer{maximum: maxStockResponseBytes}
	var stderr boundedBuffer
	stderr.maximum = 4096
	command.Stdout = responseBuffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return stockReceiveResponse{}, fmt.Errorf("git receive-pack failed: %w: %s", err, boundedText(stderr.Bytes()))
	}
	quarantineRecord, err := os.ReadFile(quarantineRecordPath)
	if err != nil || len(quarantineRecord) == 0 || len(quarantineRecord) > 4096 {
		return stockReceiveResponse{}, errors.New("quarantine ownership proof missing")
	}
	if _, err := os.Stat(string(quarantineRecord)); !errors.Is(err, os.ErrNotExist) {
		return stockReceiveResponse{}, errors.New("quarantine was not removed after receive")
	}
	if responseBuffer.overflowed {
		return stockReceiveResponse{}, errors.New("receive-pack response exceeded bound")
	}
	for _, expected := range input.Commands {
		if isZeroOID(expected.NewOID) {
			if _, err := gitOutputBounded(ctx, repoDir, 256, "show-ref", "--verify", expected.Ref); err == nil {
				return stockReceiveResponse{}, errors.New("receive-pack did not delete commanded ref")
			}
			continue
		}
		actual, err := gitOutputBounded(ctx, repoDir, 256, "rev-parse", expected.Ref)
		if err != nil || strings.TrimSpace(string(actual)) != expected.NewOID {
			return stockReceiveResponse{}, errors.New("receive-pack ref update did not match command")
		}
	}
	if err := appendStockTrace(tracePath, "disposable_ref_update_observed"); err != nil {
		return stockReceiveResponse{}, err
	}

	trace, err := readStockTrace(tracePath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	if err := validateStockTrace(trace); err != nil {
		return stockReceiveResponse{}, err
	}
	closureProof, err := readStockClosureProof(closureRecordPath)
	if err != nil || validateStockClosureProof(closureProof, manifest, input.ClosureManifestSHA256) != nil {
		return stockReceiveResponse{}, errors.New("pre-receive closure proof did not match plan")
	}
	packPath, idxPath, err := findStockOutputPack(filepath.Join(repoDir, "objects", "pack"), input.PrerequisitePackSHA256)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	if err := runGit(ctx, repoDir, "verify-pack", "-s", idxPath); err != nil {
		return stockReceiveResponse{}, err
	}
	refsPath := filepath.Join(workDir, "output.refs")
	objectCount, packSHA1, err := buildPackReferenceIndex(ctx, repoDir, packPath, idxPath, refsPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	validatedPackSHA1, err := validatePackSHA1(packPath)
	if err != nil || validatedPackSHA1 != packSHA1 {
		return stockReceiveResponse{}, errors.New("pack, index, and reference checksum binding mismatch")
	}
	packBytes, err := fileSize(packPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	idxBytes, err := fileSize(idxPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	refsBytes, err := fileSize(refsPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	packSHA256, err := sha256File(packPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	idxSHA256, err := sha256File(idxPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	refsSHA256, err := sha256File(refsPath)
	if err != nil {
		return stockReceiveResponse{}, err
	}
	if err := client.upload(ctx, input.OutputPackKey, packPath, packBytes); err != nil {
		return stockReceiveResponse{}, transientFailure("upload stock output pack", err)
	}
	if err := client.upload(ctx, input.OutputIdxKey, idxPath, idxBytes); err != nil {
		return stockReceiveResponse{}, transientFailure("upload stock output index", err)
	}
	if err := client.upload(ctx, input.OutputRefsKey, refsPath, refsBytes); err != nil {
		return stockReceiveResponse{}, transientFailure("upload stock output references", err)
	}
	return stockReceiveResponse{
		OperationID: input.OperationID, ReceivePackResponse: base64.StdEncoding.EncodeToString(responseBuffer.Bytes()),
		ReceiveResponseBytes: int64(responseBuffer.Len()), InputRequestSHA256: requestDigest,
		PackBytes: packBytes, IdxBytes: idxBytes, RefsBytes: refsBytes, PackSHA1: packSHA1,
		PackSHA256: packSHA256, IdxSHA256: idxSHA256, RefsSHA256: refsSHA256,
		ObjectCount: objectCount, InputPackObjectCount: inputPackObjectCount, Trace: trace,
		QuarantineInsideOwnedRoot: true, QuarantineRemoved: true, QuarantinePathNonEmpty: true,
		FreshWorkDirectory: true, RepositoryPackBytesBeforeHydration: repositoryPackBytesBeforeHydration,
		SharedObjectCacheDisabled: sharedCacheDisabled, SkipConnectivityCheck: skipConnectivityCheck,
		PlanSHA256: input.ClosureManifestSHA256, ClosureProof: closureProof,
	}, nil
}

func readStockInputPackObjectCount(body []byte, packOffset int64) (uint32, error) {
	if packOffset < 0 || packOffset > int64(len(body))-12 {
		return 0, errors.New("stock input pack header is truncated")
	}
	header := body[packOffset : packOffset+12]
	if string(header[:4]) != "PACK" || binary.BigEndian.Uint32(header[4:8]) != 2 {
		return 0, errors.New("stock input pack header is invalid")
	}
	count := binary.BigEndian.Uint32(header[8:12])
	if count == 0 || count > maxStockClosureObjects {
		return 0, errors.New("stock input pack object count is invalid")
	}
	return count, nil
}

func validateStockCommandPrefix(body []byte, input stockReceiveRequest) error {
	if input.PackOffset > int64(len(body)) || input.PackOffset > 256*1024 {
		return errors.New("stock receive command prefix exceeds bound")
	}
	prefix := body[:input.PackOffset]
	offset := 0
	commands := make([]receiveCommand, 0, len(input.Commands))
	for offset+4 <= len(prefix) {
		if string(prefix[offset:offset+4]) == "0000" {
			offset += 4
			break
		}
		length, err := strconv.ParseUint(string(prefix[offset:offset+4]), 16, 16)
		if err != nil || length < 4 || offset+int(length) > len(prefix) {
			return errors.New("invalid stock receive pkt-line prefix")
		}
		line := strings.TrimSuffix(string(prefix[offset+4:offset+int(length)]), "\n")
		if nul := strings.IndexByte(line, 0); nul >= 0 {
			line = line[:nul]
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return errors.New("invalid stock receive command")
		}
		commands = append(commands, receiveCommand{OldOID: fields[0], NewOID: fields[1], Ref: fields[2]})
		offset += int(length)
	}
	if offset != len(prefix) || len(commands) != len(input.Commands) {
		return errors.New("stock receive command prefix binding mismatch")
	}
	for index := range commands {
		if commands[index] != input.Commands[index] {
			return errors.New("stock receive command prefix binding mismatch")
		}
	}
	return nil
}

func indexPrerequisitePack(ctx context.Context, repoDir string, path string) error {
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()
	command := exec.CommandContext(ctx, "git", "index-pack", "--stdin", "--fix-thin", "--keep=selective-prerequisite")
	command.Dir = repoDir
	command.Env = stockGitEnvironment(ctx, repoDir)
	command.Stdin = input
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("index prerequisite pack: %w: %s", err, boundedText(output))
	}
	return nil
}

func runStockPreReceiveHook(ctx context.Context, input io.Reader) error {
	configPath := os.Getenv("DISPLAY_STOCK_RECEIVE_CONFIG")
	if configPath == "" || os.Getenv("GIT_QUARANTINE_PATH") == "" {
		return errors.New("missing stock receive hook context")
	}
	configBytes, err := os.ReadFile(configPath)
	if err != nil || int64(len(configBytes)) > maxStockMetadataBytes {
		return errors.New("invalid stock receive hook config")
	}
	var config stockHookConfig
	if err := json.Unmarshal(configBytes, &config); err != nil || config.TracePath == "" || config.ClosureRecordPath == "" || len(config.PlanSHA256) != 64 {
		return errors.New("invalid stock receive hook config")
	}
	if err := appendStockTrace(config.TracePath, "pre_receive_started"); err != nil {
		return err
	}
	quarantinePath, err := filepath.EvalSymlinks(os.Getenv("GIT_QUARANTINE_PATH"))
	if err != nil {
		return errors.New("quarantine path is not live")
	}
	gitDir := os.Getenv("GIT_DIR")
	if gitDir == "" {
		gitDir = "."
	}
	objectsCandidate, err := filepath.Abs(filepath.Join(gitDir, "objects"))
	if err != nil {
		return errors.New("repository object root is unavailable")
	}
	objectsPath, err := filepath.EvalSymlinks(objectsCandidate)
	if err != nil {
		return errors.New("repository object root is unavailable")
	}
	if err := os.Setenv("GIT_DIR", filepath.Dir(objectsPath)); err != nil {
		return errors.New("repository hook Git directory is unavailable")
	}
	relative, err := filepath.Rel(objectsPath, quarantinePath)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("quarantine path is outside the operation repository")
	}
	info, err := os.Stat(quarantinePath)
	if err != nil || !info.IsDir() {
		return errors.New("quarantine path is not a directory")
	}
	nonEmpty, err := quarantineContainsObject(quarantinePath)
	if err != nil || !nonEmpty {
		return errors.New("quarantine path contains no incoming object data")
	}
	if config.QuarantineRecordPath == "" || os.WriteFile(config.QuarantineRecordPath, []byte(quarantinePath), 0o600) != nil {
		return errors.New("quarantine ownership proof could not be recorded")
	}
	if err := appendStockTrace(config.TracePath, "pre_receive_quarantine_nonempty"); err != nil {
		return err
	}
	commands, err := readHookCommands(input)
	if err != nil {
		return err
	}
	for _, command := range commands {
		if isZeroOID(command.OldOID) {
			if _, err := gitOutputBounded(ctx, os.Getenv("GIT_DIR"), 256, "show-ref", "--verify", command.Ref); err == nil {
				return errors.New("new ref existed before closure")
			}
			continue
		}
		actual, err := gitOutputBounded(ctx, os.Getenv("GIT_DIR"), 256, "rev-parse", command.Ref)
		if err != nil || strings.TrimSpace(string(actual)) != command.OldOID {
			return errors.New("authoritative ref changed before closure")
		}
	}
	if err := appendStockTrace(config.TracePath, "logical_closure_started_ref_still_old"); err != nil {
		return err
	}
	for _, command := range commands {
		if isZeroOID(command.NewOID) {
			continue
		}
		if _, err := gitOutputBounded(ctx, os.Getenv("GIT_DIR"), 256, "cat-file", "-e", command.NewOID+"^{object}"); err != nil {
			return errors.New("incoming command object not visible in quarantine")
		}
	}
	if err := appendStockTrace(config.TracePath, "incoming_oid_visible_in_quarantine"); err != nil {
		return err
	}
	proof, err := verifyStockLogicalClosure(ctx, os.Getenv("GIT_DIR"), commands, config.AdvertisedOIDs, config.PlanSHA256)
	if err != nil {
		return err
	}
	proofBytes, err := json.Marshal(proof)
	if err != nil || int64(len(proofBytes)) > maxStockMetadataBytes || os.WriteFile(config.ClosureRecordPath, proofBytes, 0o600) != nil {
		return errors.New("logical closure proof could not be recorded")
	}
	if err := appendStockTrace(config.TracePath, "logical_closure_completed"); err != nil {
		return err
	}
	return appendStockTrace(config.TracePath, "pre_receive_succeeded")
}

func readHookCommands(input io.Reader) ([]receiveCommand, error) {
	scanner := bufio.NewScanner(io.LimitReader(input, 256*1024+1))
	commands := make([]receiveCommand, 0)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 || len(commands) >= maxCommands {
			return nil, errors.New("invalid pre-receive command")
		}
		commands = append(commands, receiveCommand{OldOID: fields[0], NewOID: fields[1], Ref: fields[2]})
	}
	if err := scanner.Err(); err != nil || len(commands) == 0 {
		return nil, errors.New("invalid pre-receive command stream")
	}
	return commands, nil
}

func quarantineContainsObject(root string) (bool, error) {
	entries := 0
	found := false
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > maxStockClosureObjects {
			return errors.New("quarantine entry bound exceeded")
		}
		if path == root || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() && info.Size() > 0 {
			found = true
			return filepath.SkipAll
		}
		return nil
	})
	return found, err
}

func verifyStockLogicalClosure(ctx context.Context, repoDir string, commands []receiveCommand, advertised []string, planSHA256 string) (stockClosureProof, error) {
	boundaries := make(map[string]struct{}, len(advertised))
	for _, oid := range advertised {
		if !objectIDPattern.MatchString(oid) {
			return stockClosureProof{}, errors.New("invalid advertised boundary")
		}
		boundaries[oid] = struct{}{}
	}
	queue := make([]string, 0, len(commands))
	for _, command := range commands {
		if !isZeroOID(command.NewOID) {
			queue = append(queue, command.NewOID)
		}
	}
	seen := make(map[string]struct{})
	external := make(map[string]struct{})
	proof := stockClosureProof{PlanSHA256: planSHA256}
	for len(queue) > 0 {
		oid := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if _, ok := seen[oid]; ok {
			continue
		}
		if len(seen) >= maxStockClosureObjects {
			return stockClosureProof{}, errors.New("logical closure object bound exceeded")
		}
		if _, boundary := boundaries[oid]; boundary {
			if _, err := gitOutputBounded(ctx, repoDir, 256, "cat-file", "-e", oid+"^{object}"); err != nil {
				return stockClosureProof{}, errors.New("advertised reachability boundary is missing")
			}
			external[oid] = struct{}{}
			seen[oid] = struct{}{}
			continue
		}
		objectTypeBytes, err := gitOutputBounded(ctx, repoDir, 64, "cat-file", "-t", oid)
		if err != nil {
			return stockClosureProof{}, errors.New("logical closure object is missing")
		}
		objectType := strings.TrimSpace(string(objectTypeBytes))
		refs, err := stockObjectReferences(ctx, repoDir, oid, objectType)
		if err != nil {
			return stockClosureProof{}, err
		}
		seen[oid] = struct{}{}
		proof.IncomingOIDs = append(proof.IncomingOIDs, oid)
		switch objectType {
		case "commit":
			proof.ObjectTypeCounts.Commit++
		case "tree":
			proof.ObjectTypeCounts.Tree++
		case "blob":
			proof.ObjectTypeCounts.Blob++
		case "tag":
			proof.ObjectTypeCounts.Tag++
		default:
			return stockClosureProof{}, errors.New("unsupported object type in logical closure")
		}
		proof.LogicalEdgeCount += len(refs)
		if proof.LogicalEdgeCount > maxStockClosureEdges {
			return stockClosureProof{}, errors.New("logical closure edge bound exceeded")
		}
		for _, referencedOID := range refs {
			if _, boundary := boundaries[referencedOID]; boundary {
				proof.ExternalEdgeCount++
				external[referencedOID] = struct{}{}
			} else {
				proof.InternalEdgeCount++
			}
			queue = append(queue, referencedOID)
		}
	}
	proof.VisitedIncomingObjectCount = len(proof.IncomingOIDs)
	proof.SemanticExternalOIDs = make([]string, 0, len(external))
	for oid := range external {
		proof.SemanticExternalOIDs = append(proof.SemanticExternalOIDs, oid)
	}
	sort.Strings(proof.IncomingOIDs)
	sort.Strings(proof.SemanticExternalOIDs)
	return proof, nil
}

func stockObjectReferences(ctx context.Context, repoDir string, oid string, objectType string) ([]string, error) {
	switch objectType {
	case "blob":
		return nil, nil
	case "commit":
		payload, err := gitOutputBounded(ctx, repoDir, maxStockMetadataObjectSize, "cat-file", "-p", oid)
		if err != nil {
			return nil, errors.New("could not read commit metadata")
		}
		refs := []string{}
		for _, line := range strings.Split(string(payload), "\n") {
			if line == "" {
				break
			}
			if strings.HasPrefix(line, "tree ") || strings.HasPrefix(line, "parent ") {
				candidate := strings.TrimSpace(strings.SplitN(line, " ", 2)[1])
				if !objectIDPattern.MatchString(candidate) {
					return nil, errors.New("invalid commit reference")
				}
				refs = append(refs, candidate)
			}
		}
		return refs, nil
	case "tag":
		payload, err := gitOutputBounded(ctx, repoDir, maxStockMetadataObjectSize, "cat-file", "-p", oid)
		if err != nil {
			return nil, errors.New("could not read tag metadata")
		}
		for _, line := range strings.Split(string(payload), "\n") {
			if strings.HasPrefix(line, "object ") {
				candidate := strings.TrimSpace(strings.TrimPrefix(line, "object "))
				if !objectIDPattern.MatchString(candidate) {
					return nil, errors.New("invalid tag reference")
				}
				return []string{candidate}, nil
			}
		}
		return nil, errors.New("tag object target missing")
	case "tree":
		payload, err := gitOutputBounded(ctx, repoDir, maxStockMetadataObjectSize, "ls-tree", "-z", oid)
		if err != nil {
			return nil, errors.New("could not read tree metadata")
		}
		refs := []string{}
		for _, entry := range bytes.Split(payload, []byte{0}) {
			if len(entry) == 0 {
				continue
			}
			header := strings.SplitN(string(entry), "\t", 2)[0]
			fields := strings.Fields(header)
			if len(fields) != 3 {
				return nil, errors.New("invalid tree entry")
			}
			if fields[0] == "160000" {
				continue
			}
			if !objectIDPattern.MatchString(fields[2]) {
				return nil, errors.New("invalid tree reference")
			}
			refs = append(refs, fields[2])
		}
		return refs, nil
	default:
		return nil, errors.New("unsupported object type in logical closure")
	}
}

func readStockClosureProof(path string) (stockClosureProof, error) {
	bytes, err := os.ReadFile(path)
	if err != nil || int64(len(bytes)) > maxStockMetadataBytes {
		return stockClosureProof{}, errors.New("logical closure proof is missing")
	}
	var proof stockClosureProof
	if err := json.Unmarshal(bytes, &proof); err != nil {
		return stockClosureProof{}, errors.New("logical closure proof is invalid")
	}
	return proof, nil
}

func validateStockClosureProof(proof stockClosureProof, manifest stockClosureManifest, planSHA256 string) error {
	if proof.PlanSHA256 != planSHA256 || proof.MissingObjectCount != 0 ||
		proof.VisitedIncomingObjectCount != manifest.Closure.VisitedIncomingObjectCount ||
		proof.LogicalEdgeCount != manifest.Closure.LogicalEdgeCount ||
		proof.InternalEdgeCount != manifest.Closure.InternalEdgeCount ||
		proof.ExternalEdgeCount != manifest.Closure.ExternalEdgeCount ||
		proof.ObjectTypeCounts != manifest.Closure.ObjectTypeCounts ||
		!reflect.DeepEqual(proof.SemanticExternalOIDs, manifest.SemanticExternalOIDs) ||
		len(proof.IncomingOIDs) != manifest.Closure.IncomingObjectCount {
		return errors.New("logical closure proof mismatch")
	}
	if err := validateSortedUniqueOIDs(proof.IncomingOIDs, false); err != nil {
		return err
	}
	if err := validateSortedUniqueOIDs(proof.SemanticExternalOIDs, true); err != nil {
		return err
	}
	return nil
}

type boundedBuffer struct {
	bytes.Buffer
	maximum    int64
	overflowed bool
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	if int64(buffer.Len()+len(value)) > buffer.maximum {
		buffer.overflowed = true
		remaining := int(buffer.maximum) - buffer.Len()
		if remaining > 0 {
			_, _ = buffer.Buffer.Write(value[:remaining])
		}
		return len(value), errors.New("bounded output exceeded")
	}
	return buffer.Buffer.Write(value)
}

func gitOutputBounded(ctx context.Context, repoDir string, maximum int64, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "git", args...)
	command.Dir = repoDir
	command.Env = stockGitEnvironment(ctx, repoDir)
	output := &boundedBuffer{maximum: maximum}
	var stderr boundedBuffer
	stderr.maximum = 2048
	command.Stdout = output
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("git command failed: %w", err)
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func appendStockTrace(path string, event string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(map[string]string{"event": event})
}

func readStockTrace(path string) ([]stockTraceEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	trace := []stockTraceEvent{}
	scanner := bufio.NewScanner(io.LimitReader(file, maxStockMetadataBytes+1))
	for scanner.Scan() {
		if len(trace) >= maxStockTraceEvents {
			return nil, errors.New("stock receive trace exceeded bound")
		}
		var raw struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil || raw.Event == "" {
			return nil, errors.New("invalid stock receive trace")
		}
		trace = append(trace, stockTraceEvent{Sequence: len(trace) + 1, Event: raw.Event})
	}
	return trace, scanner.Err()
}

func validateStockTrace(trace []stockTraceEvent) error {
	expected := []string{
		"receive_pack_invoked",
		"pre_receive_started",
		"pre_receive_quarantine_nonempty",
		"logical_closure_started_ref_still_old",
		"incoming_oid_visible_in_quarantine",
		"logical_closure_completed",
		"pre_receive_succeeded",
		"disposable_ref_update_observed",
	}
	if len(trace) != len(expected) {
		return errors.New("stock receive trace was incomplete")
	}
	for index := range expected {
		if trace[index].Event != expected[index] {
			return errors.New("stock receive trace ordering mismatch")
		}
	}
	return nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func findStockOutputPack(packDir string, prerequisiteSHA256 string) (string, string, error) {
	packs, err := filepath.Glob(filepath.Join(packDir, "pack-*.pack"))
	if err != nil {
		return "", "", err
	}
	type candidate struct{ pack, idx string }
	candidates := []candidate{}
	for _, pack := range packs {
		digest, err := sha256File(pack)
		if err != nil {
			return "", "", err
		}
		if prerequisiteSHA256 != "" && digest == prerequisiteSHA256 {
			continue
		}
		idx := strings.TrimSuffix(pack, ".pack") + ".idx"
		if _, err := os.Stat(idx); err == nil {
			candidates = append(candidates, candidate{pack: pack, idx: idx})
		}
	}
	if len(candidates) != 1 {
		return "", "", fmt.Errorf("expected one receive-pack output, found %d", len(candidates))
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].pack < candidates[j].pack })
	return candidates[0].pack, candidates[0].idx, nil
}

func validatePackSHA1(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(data) < sha1.Size {
		return "", errors.New("pack too short")
	}
	digest := sha1.Sum(data[:len(data)-sha1.Size])
	if !bytes.Equal(digest[:], data[len(data)-sha1.Size:]) {
		return "", errors.New("pack trailer mismatch")
	}
	return hex.EncodeToString(digest[:]), nil
}
