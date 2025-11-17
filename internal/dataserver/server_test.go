package dataserver

import (
	"encoding/base64"
	"os"
	"testing"

	"filefly/internal/protocol"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()

	srv, err := New(":0", dir)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	return srv
}

func TestStoreAndRetrieveWithChecksum(t *testing.T) {
	srv := newTestServer(t)
	data := []byte("hello world")

	req := protocol.DataServerRequest{
		BlockID: "block-1",
		Data:    base64.StdEncoding.EncodeToString(data),
	}

	if resp := srv.store(req); resp.Status != "ok" {
		t.Fatalf("store failed: %+v", resp)
	}

	blockPath := srv.blockPath("block-1")
	checksumPath := srv.checksumPath("block-1")

	if _, err := os.Stat(blockPath); err != nil {
		t.Fatalf("block not stored: %v", err)
	}

	if _, err := os.Stat(checksumPath); err != nil {
		t.Fatalf("checksum not stored: %v", err)
	}

	retrieveResp := srv.retrieve(protocol.DataServerRequest{BlockID: "block-1"})
	if retrieveResp.Status != "ok" {
		t.Fatalf("retrieve failed: %+v", retrieveResp)
	}

	decoded, err := base64.StdEncoding.DecodeString(retrieveResp.Data)
	if err != nil {
		t.Fatalf("decode data: %v", err)
	}

	if string(decoded) != string(data) {
		t.Fatalf("unexpected data: got %q want %q", decoded, data)
	}
}

func TestRetrieveChecksumMismatch(t *testing.T) {
	srv := newTestServer(t)
	data := []byte("original data")

	req := protocol.DataServerRequest{
		BlockID: "block-2",
		Data:    base64.StdEncoding.EncodeToString(data),
	}

	if resp := srv.store(req); resp.Status != "ok" {
		t.Fatalf("store failed: %+v", resp)
	}

	// Corrupt the block data to trigger a checksum mismatch.
	if err := os.WriteFile(srv.blockPath("block-2"), []byte("tampered"), 0o644); err != nil {
		t.Fatalf("tamper block: %v", err)
	}

	resp := srv.retrieve(protocol.DataServerRequest{BlockID: "block-2"})
	if resp.Status != "error" {
		t.Fatalf("expected error, got %+v", resp)
	}

	if resp.Error != "checksum mismatch" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestRetrieveMissingChecksum(t *testing.T) {
	srv := newTestServer(t)

	req := protocol.DataServerRequest{
		BlockID: "block-3",
		Data:    base64.StdEncoding.EncodeToString([]byte("data")),
	}

	if resp := srv.store(req); resp.Status != "ok" {
		t.Fatalf("store failed: %+v", resp)
	}

	if err := os.Remove(srv.checksumPath("block-3")); err != nil {
		t.Fatalf("remove checksum: %v", err)
	}

	resp := srv.retrieve(protocol.DataServerRequest{BlockID: "block-3"})
	if resp.Status != "error" {
		t.Fatalf("expected error, got %+v", resp)
	}

	if resp.Error != "checksum file missing" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
}

func TestDeleteRemovesChecksum(t *testing.T) {
	srv := newTestServer(t)

	req := protocol.DataServerRequest{
		BlockID: "block-4",
		Data:    base64.StdEncoding.EncodeToString([]byte("data")),
	}

	if resp := srv.store(req); resp.Status != "ok" {
		t.Fatalf("store failed: %+v", resp)
	}

	if resp := srv.delete(protocol.DataServerRequest{BlockID: "block-4"}); resp.Status != "ok" {
		t.Fatalf("delete failed: %+v", resp)
	}

	if _, err := os.Stat(srv.blockPath("block-4")); !os.IsNotExist(err) {
		t.Fatalf("block file still exists: %v", err)
	}

	if _, err := os.Stat(srv.checksumPath("block-4")); !os.IsNotExist(err) {
		t.Fatalf("checksum file still exists: %v", err)
	}
}
