package metadataserver

import (
	"path/filepath"
	"testing"
	"time"

	"filefly/internal/protocol"
)

func TestAdvertisedMetadataUsesPublicAddresses(t *testing.T) {
	server := New(
		":0",
		1024,
		[]string{"10.0.0.5:9000"},
		filepath.Join(t.TempDir(), "metadata.json"),
		time.Minute,
		0,
	)

	server.setAdvertisedMapping("10.0.0.5:9000", "203.0.113.5:9000")

	original := protocol.FileMetadata{
		Name:      "example",
		TotalSize: 4,
		Blocks: []protocol.BlockRef{{
			ID:   "example-0",
			Size: 4,
			Replicas: []protocol.BlockReplica{{
				DataServer: "10.0.0.5:9000",
			}},
		}},
	}

	advertised := server.advertiseMetadata(&original)

	if advertised == nil {
		t.Fatalf("expected advertised metadata")
	}

	if advertised.Blocks[0].Replicas[0].DataServer != "203.0.113.5:9000" {
		t.Fatalf("expected advertised data server, got %s", advertised.Blocks[0].Replicas[0].DataServer)
	}

	if original.Blocks[0].Replicas[0].DataServer != "10.0.0.5:9000" {
		t.Fatalf("expected original metadata to remain unchanged")
	}
}

func TestCompleteFilePersistsInternalAddresses(t *testing.T) {
	server := New(
		":0",
		1024,
		nil,
		filepath.Join(t.TempDir(), "metadata.json"),
		time.Minute,
		0,
	)

	registerResp := server.registerDataServer(protocol.MetadataRequest{
		DataServerAddr: "10.0.0.8:9100",
		AdvertisedAddr: "203.0.113.8:9100",
	})

	if registerResp.Status != "ok" {
		t.Fatalf("register data server failed: %s", registerResp.Error)
	}

	clientMeta := protocol.FileMetadata{
		Name:      "example",
		TotalSize: 4,
		Blocks: []protocol.BlockRef{{
			ID:   "example-0",
			Size: 4,
			Replicas: []protocol.BlockReplica{{
				DataServer: "203.0.113.8:9100",
			}},
		}},
	}

	resp := server.completeFile(protocol.MetadataRequest{Metadata: &clientMeta})

	if resp.Status != "ok" {
		t.Fatalf("completeFile failed: %s", resp.Error)
	}

	stored, ok := server.store.Get("example")
	if !ok {
		t.Fatalf("stored metadata not found")
	}

	if stored.Blocks[0].Replicas[0].DataServer != "10.0.0.8:9100" {
		t.Fatalf("expected internal address to be persisted, got %s", stored.Blocks[0].Replicas[0].DataServer)
	}

	if resp.Metadata == nil {
		t.Fatalf("expected metadata returned to client")
	}

	if resp.Metadata.Blocks[0].Replicas[0].DataServer != "203.0.113.8:9100" {
		t.Fatalf("expected advertised address in response, got %s", resp.Metadata.Blocks[0].Replicas[0].DataServer)
	}
}
