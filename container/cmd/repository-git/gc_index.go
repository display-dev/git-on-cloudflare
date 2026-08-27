package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// A maintenance input supplies protected roots, not receive commands. All ref
// writes below are confined to a fresh temporary bare object store.
type gcIndexRequest struct {
	Roots           []string `json:"roots"`
	ObjectCount     uint32   `json:"objectCount"`
	ObjectSetSHA256 string   `json:"objectSetSha256"`
	PackSHA1        string   `json:"packSha1"`
	ResultKey       string   `json:"resultKey"`
}

type gcIndexResult struct {
	ObjectSetSHA256  string `json:"objectSetSha256"`
	DownloadMS       int64  `json:"downloadMs"`
	IndexMS          int64  `json:"indexMs"`
	ValidationMS     int64  `json:"validationMs"`
	ReferenceMS      int64  `json:"referenceMs"`
	UploadMS         int64  `json:"uploadMs"`
	DownloadBytes    int64  `json:"downloadBytes"`
	UploadBytes      int64  `json:"uploadBytes"`
	DownloadRequests int    `json:"downloadRequests"`
	UploadRequests   int    `json:"uploadRequests"`
}

func validateGcIndexRequest(input processRequest) error {
	gc := input.Maintenance
	if gc == nil || len(input.Commands) != 0 || len(input.ActivePacks) != 0 || len(gc.Roots) == 0 || len(gc.Roots) > maxCommands || gc.ObjectCount == 0 {
		return errors.New("invalid maintenance input")
	}
	digest, err := hex.DecodeString(gc.ObjectSetSHA256)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != gc.ObjectSetSHA256 || !objectIDPattern.MatchString(gc.PackSHA1) {
		return errors.New("invalid maintenance identity")
	}
	keys := map[string]bool{}
	for _, key := range []string{input.InputPackKey, input.OutputPackKey, input.OutputIdxKey, input.OutputRefsKey, gc.ResultKey} {
		if key == "" || keys[key] {
			return errors.New("maintenance artifacts must have separate ownership")
		}
		keys[key] = true
	}
	for _, root := range gc.Roots {
		if !objectIDPattern.MatchString(root) {
			return errors.New("invalid maintenance root")
		}
	}
	return nil
}

func verifyGcIndex(ctx context.Context, repoDir, packPath, idxPath string, input processRequest) error {
	gc := input.Maintenance
	data, err := os.ReadFile(idxPath)
	if err != nil {
		return err
	}
	oids, checksum, _, err := parseIndex(data)
	if err != nil {
		return err
	}
	if len(oids) != int(gc.ObjectCount) || hex.EncodeToString(checksum) != gc.PackSHA1 {
		return errors.New("maintenance pack identity mismatch")
	}
	packBytes, err := fileSize(packPath)
	if err != nil {
		return err
	}
	if packBytes != input.InputBytes {
		return errors.New("maintenance indexing changed pack bytes")
	}
	digest := sha256.New()
	for _, oid := range oids {
		fmt.Fprintf(digest, "%s\n", hex.EncodeToString(oid))
	}
	if hex.EncodeToString(digest.Sum(nil)) != gc.ObjectSetSHA256 {
		return errors.New("maintenance object set mismatch")
	}
	for index, root := range gc.Roots {
		if err := runGit(ctx, repoDir, "update-ref", fmt.Sprintf("refs/gc-roots/%d", index), root); err != nil {
			return err
		}
	}
	// index-pack verifies the pack checksum; strict fsck also validates object
	// bodies and connectivity. Expected membership came from the fenced source
	// closure, not from this processor or the mutable current ref advertisement.
	return runGit(ctx, repoDir, "fsck", "--full", "--strict")
}

func uploadGcResult(ctx context.Context, client objectBridge, workDir, key string, result processResponse) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	path := filepath.Join(workDir, "maintenance-result.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	return client.upload(ctx, key, path, int64(len(data)))
}
