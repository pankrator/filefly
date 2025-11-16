package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"filefly/internal/protocol"
	"filefly/internal/transfer"
)

func main() {
	metadataAddr := flag.String("metadata-server", ":9000", "address of the metadata server")
	filePath := flag.String("file", "", "path to the file to upload")
	fileName := flag.String("name", "", "remote file name (defaults to the base name of --file)")
	replicas := flag.Int("replicas", 1, "number of replicas per block")
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

	plan, err := requestPlan(*metadataAddr, *fileName, len(data), *replicas)
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

	client := transfer.NewClient()
	defer client.Close()

	if err := client.UploadBlocks(plan.Metadata.Blocks, data); err != nil {
		log.Fatalf("upload blocks: %v", err)
	}

	log.Printf("uploaded %s (%d bytes) in %d blocks", plan.Metadata.Name, plan.Metadata.TotalSize, len(plan.Metadata.Blocks))
}

func requestPlan(addr, fileName string, size, replicas int) (*protocol.MetadataResponse, error) {
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
		Replicas: replicas,
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
