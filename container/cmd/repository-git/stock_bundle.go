package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	stockBundleHeaderBytes   = int64(64 * 1024)
	stockBundleInputBytes    = int64(48 * 1024 * 1024)
	stockBundleOutputBytes   = int64(96 * 1024 * 1024)
	stockBundleArtifactBytes = int64(32 * 1024 * 1024)
)

var (
	stockBundleRequestMagic  = [8]byte{'S', 'T', 'K', 'R', 'E', 'Q', '1', '\n'}
	stockBundleResponseMagic = [8]byte{'S', 'T', 'K', 'O', 'U', 'T', '1', '\n'}
)

type stockBundleObject struct {
	path  string
	bytes int64
}

type stockBundleBridge struct {
	inputs  map[string]stockBundleObject
	outputs map[string]stockBundleObject
	root    string
}

func copyExactFile(source string, destination string, expected int64) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.CopyN(output, input, expected)
	extra := make([]byte, 1)
	extraBytes, extraErr := input.Read(extra)
	closeErr := output.Close()
	if copyErr != nil || written != expected || extraBytes != 0 || (extraErr != nil && !errors.Is(extraErr, io.EOF)) || closeErr != nil {
		_ = os.Remove(destination)
		return errors.New("bundle file length mismatch")
	}
	return nil
}

func (bridge *stockBundleBridge) download(_ context.Context, key string, destination string, expected int64) error {
	object, ok := bridge.inputs[key]
	if !ok || object.bytes != expected {
		return errors.New("bundle input is not authorized")
	}
	return copyExactFile(object.path, destination, expected)
}

func (bridge *stockBundleBridge) upload(_ context.Context, key string, source string, expected int64) error {
	if expected <= 0 || expected > stockBundleArtifactBytes {
		return errors.New("bundle output exceeds its bound")
	}
	if _, duplicate := bridge.outputs[key]; duplicate {
		return errors.New("bundle output key was reused")
	}
	destination := filepath.Join(bridge.root, fmt.Sprintf("output-%d.bin", len(bridge.outputs)))
	if err := copyExactFile(source, destination, expected); err != nil {
		return err
	}
	bridge.outputs[key] = stockBundleObject{path: destination, bytes: expected}
	return nil
}

func decodeStockBundleHeader(reader *bufio.Reader) (stockReceiveRequest, int64, error) {
	var magic [8]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil || magic != stockBundleRequestMagic {
		return stockReceiveRequest{}, 0, errors.New("invalid stock bundle magic")
	}
	var lengthBytes [4]byte
	if _, err := io.ReadFull(reader, lengthBytes[:]); err != nil {
		return stockReceiveRequest{}, 0, errors.New("stock bundle header is truncated")
	}
	headerLength := int64(binary.BigEndian.Uint32(lengthBytes[:]))
	if headerLength <= 0 || headerLength > stockBundleHeaderBytes {
		return stockReceiveRequest{}, 0, errors.New("stock bundle header exceeds its bound")
	}
	header := make([]byte, headerLength)
	if _, err := io.ReadFull(reader, header); err != nil {
		return stockReceiveRequest{}, 0, errors.New("stock bundle header is truncated")
	}
	decoder := json.NewDecoder(bytes.NewReader(header))
	decoder.DisallowUnknownFields()
	var input stockReceiveRequest
	if err := decoder.Decode(&input); err != nil {
		return stockReceiveRequest{}, 0, errors.New("stock bundle header is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return stockReceiveRequest{}, 0, errors.New("stock bundle header has trailing data")
	}
	if err := validateStockReceiveRequest(input); err != nil {
		return stockReceiveRequest{}, 0, err
	}
	return input, headerLength, nil
}

func receiveStockBundleSection(reader io.Reader, path string, expected int64) error {
	if expected <= 0 || expected > stockBundleArtifactBytes {
		return errors.New("stock bundle input exceeds its bound")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	written, copyErr := io.CopyN(file, reader, expected)
	closeErr := file.Close()
	if copyErr != nil || written != expected || closeErr != nil {
		_ = os.Remove(path)
		return errors.New("stock bundle input is truncated")
	}
	return nil
}

func writeStockBundleFile(writer io.Writer, object stockBundleObject) error {
	file, err := os.Open(object.path)
	if err != nil {
		return err
	}
	defer file.Close()
	written, err := io.CopyN(writer, file, object.bytes)
	if err != nil || written != object.bytes {
		return errors.New("stock bundle output is truncated")
	}
	var trailing [1]byte
	if count, trailingErr := file.Read(trailing[:]); count != 0 || (trailingErr != nil && !errors.Is(trailingErr, io.EOF)) {
		return errors.New("stock bundle output has trailing bytes")
	}
	return nil
}

func stockReceiveBundleHandler(writer http.ResponseWriter, request *http.Request) {
	maximumRequest := int64(12) + stockBundleHeaderBytes + stockBundleInputBytes
	if request.ContentLength <= 0 || request.ContentLength > maximumRequest {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid stock bundle declaration")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maximumRequest)
	reader := bufio.NewReaderSize(request.Body, 64*1024)
	input, headerLength, err := decodeStockBundleHeader(reader)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "invalid stock bundle header")
		return
	}
	expectedRequest := int64(12) + headerLength + input.InputRequestBytes + input.PrerequisitePackBytes + input.ClosureManifestBytes
	if request.ContentLength != expectedRequest || expectedRequest > maximumRequest {
		writeError(writer, http.StatusBadRequest, "invalid_request", "stock bundle length mismatch")
		return
	}
	// RepoDO admits at most eight stock preparations. Each uses a fresh bundle
	// root, repository, Git config, hooks, and quarantine, so the dedicated stock
	// Container can process that bounded set in parallel. Generic and maintenance
	// Container applications retain their separate single-operation guard.
	if !acquireStockProcessSlot() {
		writeProcessError(writer, transientFailure("repository processor busy", errors.New("another operation is active")))
		return
	}
	defer releaseStockProcessSlot()

	bundleRoot, err := os.MkdirTemp("", "repository-stock-bundle-")
	if err != nil {
		writeProcessError(writer, err)
		return
	}
	defer os.RemoveAll(bundleRoot)
	inputPath := filepath.Join(bundleRoot, "input-request.bin")
	prerequisitePath := filepath.Join(bundleRoot, "prerequisite.pack")
	manifestPath := filepath.Join(bundleRoot, "closure-manifest.json")
	if receiveStockBundleSection(reader, inputPath, input.InputRequestBytes) != nil ||
		receiveStockBundleSection(reader, prerequisitePath, input.PrerequisitePackBytes) != nil ||
		receiveStockBundleSection(reader, manifestPath, input.ClosureManifestBytes) != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "stock bundle input is truncated")
		return
	}
	var trailing [1]byte
	if count, trailingErr := reader.Read(trailing[:]); count != 0 || (trailingErr != nil && !errors.Is(trailingErr, io.EOF)) {
		writeError(writer, http.StatusBadRequest, "invalid_request", "stock bundle input has trailing bytes")
		return
	}
	bridge := &stockBundleBridge{
		inputs: map[string]stockBundleObject{
			input.InputRequestKey:     {path: inputPath, bytes: input.InputRequestBytes},
			input.PrerequisitePackKey: {path: prerequisitePath, bytes: input.PrerequisitePackBytes},
			input.ClosureManifestKey:  {path: manifestPath, bytes: input.ClosureManifestBytes},
		},
		outputs: make(map[string]stockBundleObject, 3),
		root:    bundleRoot,
	}
	startedAt := time.Now()
	processingContext, cancelProcessing := context.WithTimeout(
		context.Background(),
		containerHTTPClient,
	)
	defer cancelProcessing()
	result, err := processStockReceive(processingContext, input, bridge)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repository-git: stock bundle failed: %s\n", processErrorCategory(err))
		writeProcessError(writer, err)
		return
	}
	result.ElapsedMS = time.Since(startedAt).Milliseconds()
	resultBytes, err := json.Marshal(result)
	if err != nil || int64(len(resultBytes)) <= 0 || int64(len(resultBytes)) > maxStockResponseBytes {
		writeProcessError(writer, errors.New("stock result metadata exceeds its bound"))
		return
	}
	pack, packOK := bridge.outputs[input.OutputPackKey]
	idx, idxOK := bridge.outputs[input.OutputIdxKey]
	refs, refsOK := bridge.outputs[input.OutputRefsKey]
	result.ResultKind = normalizedStockResultKind(result.ResultKind)
	if result.ResultKind == "ref-only" {
		if packOK || idxOK || refsOK || result.PackBytes != 0 || result.IdxBytes != 0 || result.RefsBytes != 0 || result.ObjectCount != 0 {
			writeProcessError(writer, errors.New("ref-only stock result unexpectedly produced outputs"))
			return
		}
		pack, idx, refs = stockBundleObject{}, stockBundleObject{}, stockBundleObject{}
	} else if !packOK || !idxOK || !refsOK || pack.bytes != result.PackBytes || idx.bytes != result.IdxBytes || refs.bytes != result.RefsBytes {
		writeProcessError(writer, errors.New("stock result outputs are incomplete"))
		return
	}
	responseBytes := int64(12+len(resultBytes)) + pack.bytes + idx.bytes + refs.bytes
	if responseBytes > int64(12)+maxStockResponseBytes+stockBundleOutputBytes {
		writeProcessError(writer, errors.New("stock bundle response exceeds its bound"))
		return
	}
	writer.Header().Set("Content-Type", "application/x-display-stock-receive-output")
	writer.Header().Set("Content-Length", fmt.Sprintf("%d", responseBytes))
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write(stockBundleResponseMagic[:]); err != nil {
		return
	}
	var lengthBytes [4]byte
	binary.BigEndian.PutUint32(lengthBytes[:], uint32(len(resultBytes)))
	if _, err := writer.Write(lengthBytes[:]); err != nil {
		return
	}
	if _, err := writer.Write(resultBytes); err != nil {
		return
	}
	if result.ResultKind == "artifacts" {
		for _, object := range []stockBundleObject{pack, idx, refs} {
			if writeStockBundleFile(writer, object) != nil {
				return
			}
		}
	}
}

func normalizedStockResultKind(resultKind string) string {
	if resultKind == "" {
		return "artifacts"
	}
	return resultKind
}
