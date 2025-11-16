package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"filefly/internal/protocol"
)

func main() {
	metadataAddr := flag.String("metadata-addr", "localhost:9000", "address of the metadata server")
	filePath := flag.String("file", "", "path to the file that should be uploaded")
	fileName := flag.String("file-name", "", "optional logical name for the file; defaults to the basename of --file")
	flag.Parse()

	if *filePath == "" {
		log.Fatal("missing --file path to upload")
	}

	contents, err := os.ReadFile(*filePath)
	if err != nil {
		log.Fatalf("read file: %v", err)
	}

	name := *fileName
	if name == "" {
		name = filepath.Base(*filePath)
	}

	meta, err := requestPlan(*metadataAddr, name, len(contents))
	if err != nil {
		log.Fatalf("request upload plan: %v", err)
	}
	if meta == nil || meta.Metadata == nil {
		log.Fatalf("metadata server did not return a plan")
	}

	if err := uploadBlocks(meta.Metadata.Blocks, contents); err != nil {
		log.Fatalf("upload blocks: %v", err)
	}

	log.Printf("uploaded %s (%d bytes) in %d blocks", name, len(contents), len(meta.Metadata.Blocks))
}

func requestPlan(addr, fileName string, fileSize int) (*protocol.MetadataResponse, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to metadata server %s: %w", addr, err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.MetadataRequest{
		Command:  "store_file",
		FileName: fileName,
		FileSize: fileSize,
	}
	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("send store_file request: %w", err)
	}

	var resp protocol.MetadataResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode metadata response: %w", err)
	}
	if resp.Status != "ok" {
		if resp.Error != "" {
			return nil, fmt.Errorf("metadata server error: %s", resp.Error)
		}
		return nil, fmt.Errorf("metadata server returned status %s", resp.Status)
	}
	return &resp, nil
}

func uploadBlocks(blocks []protocol.BlockRef, data []byte) error {
	offset := 0
	for _, block := range blocks {
		end := offset + block.Size
		if end > len(data) {
			return fmt.Errorf("block %s exceeds file size", block.ID)
		}
		chunk := data[offset:end]
		offset = end
		if err := sendBlock(block.DataServer, block.ID, chunk); err != nil {
			return err
		}
		log.Printf("sent block %s (%d bytes) to %s", block.ID, len(chunk), block.DataServer)
	}
	if offset != len(data) {
		return fmt.Errorf("plan only covered %d of %d bytes", offset, len(data))
	}
	return nil
}

func sendBlock(addr, blockID string, chunk []byte) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))

	req := protocol.DataServerRequest{
		Command: "store",
		BlockID: blockID,
		Data:    base64.StdEncoding.EncodeToString(chunk),
	}
	if err := enc.Encode(req); err != nil {
		return fmt.Errorf("send block %s: %w", blockID, err)
	}

	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return fmt.Errorf("decode response for block %s: %w", blockID, err)
	}
	if resp.Status != "ok" {
		if resp.Error != "" {
			return fmt.Errorf("data server %s error for block %s: %s", addr, blockID, resp.Error)
		}
		return fmt.Errorf("data server %s returned status %s", addr, resp.Status)
	}
	return nil
}
