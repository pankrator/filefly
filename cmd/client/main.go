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
	metadataAddr := flag.String("metadata-server", ":9000", "address of the metadata server")
	filePath := flag.String("file", "", "path to the file to upload")
	fileName := flag.String("name", "", "remote file name (defaults to the base name of --file)")
	flag.Parse()

	if *filePath == "" {
		log.Fatal("--file is required")
	}

	if *fileName == "" {
		*fileName = filepath.Base(*filePath)
	}

	data, err := os.ReadFile(*filePath)
	if err != nil {
		log.Fatalf("read file: %v", err)
	}

	plan, err := requestPlan(*metadataAddr, *fileName, len(data))
	if err != nil {
		log.Fatalf("request plan: %v", err)
	}

	if plan.Metadata == nil {
		log.Fatalf("metadata server did not return metadata for %s", *fileName)
	}

	if len(plan.Metadata.Blocks) == 0 {
		log.Printf("file %s has no data to upload", plan.Metadata.Name)
		return
	}

	if err := uploadBlocks(plan.Metadata.Blocks, data); err != nil {
		log.Fatalf("upload blocks: %v", err)
	}

	log.Printf("uploaded %s (%d bytes) in %d blocks", plan.Metadata.Name, plan.Metadata.TotalSize, len(plan.Metadata.Blocks))
}

func requestPlan(addr, fileName string, size int) (*protocol.MetadataResponse, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to metadata server: %w", err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.MetadataRequest{
		Command:  "store_file",
		FileName: fileName,
		FileSize: size,
	}
	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("send store_file: %w", err)
	}

	var resp protocol.MetadataResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if resp.Status != "ok" {
		if resp.Error == "" {
			resp.Error = "metadata server returned error"
		}
		return nil, fmt.Errorf(resp.Error)
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
		if err := uploadBlock(block, chunk); err != nil {
			return err
		}
		offset = end
	}
	if offset != len(data) {
		return fmt.Errorf("plan left %d bytes unused", len(data)-offset)
	}
	return nil
}

func uploadBlock(block protocol.BlockRef, data []byte) error {
	conn, err := net.Dial("tcp", block.DataServer)
	if err != nil {
		return fmt.Errorf("connect to data server %s: %w", block.DataServer, err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))

	req := protocol.DataServerRequest{
		Command: "store",
		BlockID: block.ID,
		Data:    base64.StdEncoding.EncodeToString(data),
	}
	if err := enc.Encode(req); err != nil {
		return fmt.Errorf("send store to %s: %w", block.DataServer, err)
	}

	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return fmt.Errorf("decode response from %s: %w", block.DataServer, err)
	}
	if resp.Status != "ok" {
		if resp.Error == "" {
			resp.Error = "data server returned error"
		}
		return fmt.Errorf("data server %s: %s", block.DataServer, resp.Error)
	}

	return nil
}
