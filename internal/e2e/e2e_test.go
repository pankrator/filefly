package e2e

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filefly/internal/dataserver"
	"filefly/internal/metadataserver"
	"filefly/internal/protocol"
)

func TestEndToEndUploadAndFetchSuccess(t *testing.T) {
	dataServer := startDataServer(t)
	metadataAddr := startMetadataServer(t, 8, []dataServerHandle{dataServer})

	payload := []byte("hello from filefly")
	storeResp := sendMetadataRequest(t, metadataAddr, protocol.MetadataRequest{
		Command:  "store_file",
		FileName: "success.txt",
		FileSize: len(payload),
	})
	if storeResp.Status != "ok" {
		t.Fatalf("store_file failed: %s", storeResp.Error)
	}
	writeBlocks(t, storeResp.Metadata, payload)

	fetchResp := sendMetadataRequest(t, metadataAddr, protocol.MetadataRequest{
		Command:  "fetch_file",
		FileName: "success.txt",
	})
	if fetchResp.Status != "ok" {
		t.Fatalf("fetch_file failed: %s", fetchResp.Error)
	}
	data, err := base64.StdEncoding.DecodeString(fetchResp.Data)
	if err != nil {
		t.Fatalf("decode fetch payload: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("unexpected data: got %q want %q", data, payload)
	}
	if fetchResp.Metadata == nil || fetchResp.Metadata.Name != "success.txt" {
		t.Fatalf("metadata missing from fetch response")
	}
}

func TestEndToEndFetchFailsWhenBlocksMissing(t *testing.T) {
	dataServer := startDataServer(t)
	metadataAddr := startMetadataServer(t, 6, []dataServerHandle{dataServer})

	payload := []byte("missing blocks")
	storeResp := sendMetadataRequest(t, metadataAddr, protocol.MetadataRequest{
		Command:  "store_file",
		FileName: "missing.txt",
		FileSize: len(payload),
	})
	if storeResp.Status != "ok" {
		t.Fatalf("store_file failed: %s", storeResp.Error)
	}

	fetchResp := sendMetadataRequest(t, metadataAddr, protocol.MetadataRequest{
		Command:  "fetch_file",
		FileName: "missing.txt",
	})
	if fetchResp.Status != "error" {
		t.Fatalf("expected fetch to fail when blocks are absent")
	}
	if !strings.Contains(fetchResp.Error, "block") {
		t.Fatalf("unexpected error: %s", fetchResp.Error)
	}
}

func TestEndToEndReplicaResiliency(t *testing.T) {
	primary := startDataServer(t)
	replica := startDataServer(t)
	metadataAddr := startMetadataServer(t, 16, []dataServerHandle{primary, replica})

	payload := []byte("replicated data")
	storeResp := sendMetadataRequest(t, metadataAddr, protocol.MetadataRequest{
		Command:  "store_file",
		FileName: "resilient.txt",
		FileSize: len(payload),
		Replicas: 2,
	})
	if storeResp.Status != "ok" {
		t.Fatalf("store_file failed: %s", storeResp.Error)
	}
	if len(storeResp.Metadata.Blocks) != 1 {
		t.Fatalf("expected a single block, got %d", len(storeResp.Metadata.Blocks))
	}
	if len(storeResp.Metadata.Blocks[0].Replicas) < 2 {
		t.Fatalf("block lacks replicas: %#v", storeResp.Metadata.Blocks[0])
	}

	writeBlocks(t, storeResp.Metadata, payload)

	firstReplica := storeResp.Metadata.Blocks[0].Replicas[0]
	if firstReplica.DataServer != primary.addr {
		t.Fatalf("unexpected replica ordering: %v", firstReplica)
	}
	removeBlockFile(t, primary.storageDir, storeResp.Metadata.Blocks[0].ID)

	fetchResp := sendMetadataRequest(t, metadataAddr, protocol.MetadataRequest{
		Command:  "fetch_file",
		FileName: "resilient.txt",
	})
	if fetchResp.Status != "ok" {
		t.Fatalf("fetch_file failed despite replica: %s", fetchResp.Error)
	}
	data, err := base64.StdEncoding.DecodeString(fetchResp.Data)
	if err != nil {
		t.Fatalf("decode fetch payload: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("unexpected data via replica: got %q want %q", data, payload)
	}
}

type dataServerHandle struct {
	addr       string
	storageDir string
}

func startDataServer(tb testing.TB) dataServerHandle {
	tb.Helper()
	addr := randomAddr(tb)
	dir := tb.TempDir()
	srv, err := dataserver.New(addr, dir)
	if err != nil {
		tb.Fatalf("create data server: %v", err)
	}
	go func() {
		if err := srv.Listen(); err != nil {
			panic(fmt.Sprintf("data server %s failed: %v", addr, err))
		}
	}()
	waitForServer(tb, addr)
	return dataServerHandle{addr: addr, storageDir: dir}
}

func startMetadataServer(tb testing.TB, blockSize int, servers []dataServerHandle) string {
	tb.Helper()
	addr := randomAddr(tb)
	addrs := make([]string, len(servers))
	for i, srv := range servers {
		addrs[i] = srv.addr
	}
	meta := metadataserver.New(addr, blockSize, addrs, "", 0)
	go func() {
		if err := meta.Listen(); err != nil {
			panic(fmt.Sprintf("metadata server %s failed: %v", addr, err))
		}
	}()
	waitForServer(tb, addr)
	return addr
}

func randomAddr(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve tcp port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func waitForServer(tb testing.TB, addr string) {
	tb.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("server at %s did not start", addr)
}

func sendMetadataRequest(tb testing.TB, addr string, req protocol.MetadataRequest) protocol.MetadataResponse {
	tb.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		tb.Fatalf("dial metadata server: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		tb.Fatalf("encode metadata request: %v", err)
	}
	var resp protocol.MetadataResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		tb.Fatalf("decode metadata response: %v", err)
	}
	return resp
}

func sendDataServerRequest(tb testing.TB, addr string, req protocol.DataServerRequest) protocol.DataServerResponse {
	tb.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		tb.Fatalf("dial data server %s: %v", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		tb.Fatalf("encode data request: %v", err)
	}
	var resp protocol.DataServerResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		tb.Fatalf("decode data response: %v", err)
	}
	return resp
}

func writeBlocks(tb testing.TB, meta *protocol.FileMetadata, payload []byte) {
	tb.Helper()
	if meta == nil {
		tb.Fatalf("metadata missing")
	}
	if len(payload) != meta.TotalSize {
		tb.Fatalf("payload size mismatch: got %d want %d", len(payload), meta.TotalSize)
	}
	offset := 0
	for _, block := range meta.Blocks {
		end := offset + block.Size
		if end > len(payload) {
			tb.Fatalf("block %s exceeds payload bounds", block.ID)
		}
		chunk := payload[offset:end]
		offset = end
		encoded := base64.StdEncoding.EncodeToString(chunk)
		for _, replica := range block.Replicas {
			resp := sendDataServerRequest(tb, replica.DataServer, protocol.DataServerRequest{
				Command: "store",
				BlockID: block.ID,
				Data:    encoded,
			})
			if resp.Status != "ok" {
				tb.Fatalf("store block %s on %s failed: %s", block.ID, replica.DataServer, resp.Error)
			}
		}
	}
}

func removeBlockFile(tb testing.TB, storageDir, blockID string) {
	tb.Helper()
	path := blockFilePath(storageDir, blockID)
	if err := os.Remove(path); err != nil {
		tb.Fatalf("remove block %s from %s: %v", blockID, storageDir, err)
	}
}

func blockFilePath(dir, blockID string) string {
	safe := base64.RawURLEncoding.EncodeToString([]byte(blockID))
	return filepath.Join(dir, safe)
}
