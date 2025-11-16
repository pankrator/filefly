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

	if err := uploadBlocks(plan.Metadata.Blocks, data); err != nil {
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

func uploadBlocks(blocks []protocol.BlockRef, data []byte) error {
	offset := 0
	conns := newConnCache()
	defer conns.CloseAll()
	for _, block := range blocks {
		end := offset + block.Size
		if end > len(data) {
			return fmt.Errorf("block %s exceeds file size", block.ID)
		}
		chunk := data[offset:end]
		replicas := block.Replicas
		if len(replicas) == 0 && block.DataServer != "" {
			replicas = []protocol.BlockReplica{{DataServer: block.DataServer}}
		}
		if len(replicas) == 0 {
			return fmt.Errorf("block %s has no replicas to upload", block.ID)
		}
		for _, replica := range replicas {
			if err := uploadReplica(block.ID, replica, chunk, conns); err != nil {
				return err
			}
		}
		offset = end
	}
	if offset != len(data) {
		return fmt.Errorf("plan left %d bytes unused", len(data)-offset)
	}
	return nil
}

func uploadReplica(blockID string, replica protocol.BlockReplica, data []byte, cache *connCache) error {
	conn, err := cache.Get(replica.DataServer)
	if err != nil {
		return fmt.Errorf("connect to data server %s: %w", replica.DataServer, err)
	}
	req := protocol.DataServerRequest{
		Command: "store",
		BlockID: blockID,
		Data:    base64.StdEncoding.EncodeToString(data),
	}
	if err := conn.enc.Encode(req); err != nil {
		cache.Drop(replica.DataServer)
		return fmt.Errorf("send store to %s: %w", replica.DataServer, err)
	}

	var resp protocol.DataServerResponse
	if err := conn.dec.Decode(&resp); err != nil {
		cache.Drop(replica.DataServer)
		return fmt.Errorf("decode response from %s: %w", replica.DataServer, err)
	}
	if resp.Status != "ok" {
		if resp.Error == "" {
			resp.Error = "data server returned error"
		}
		return fmt.Errorf("data server %s: %s", replica.DataServer, resp.Error)
	}

	return nil
}

type cachedConn struct {
	enc  *json.Encoder
	dec  *json.Decoder
	conn net.Conn
}

type connCache struct {
	conns map[string]*cachedConn
}

func newConnCache() *connCache {
	return &connCache{conns: make(map[string]*cachedConn)}
}

func (c *connCache) Get(addr string) (*cachedConn, error) {
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	netConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	cached := &cachedConn{
		conn: netConn,
		enc:  json.NewEncoder(netConn),
		dec:  json.NewDecoder(bufio.NewReader(netConn)),
	}
	c.conns[addr] = cached
	return cached, nil
}

func (c *connCache) Drop(addr string) {
	if conn, ok := c.conns[addr]; ok {
		_ = conn.conn.Close()
		delete(c.conns, addr)
	}
}

func (c *connCache) CloseAll() {
	for addr := range c.conns {
		c.Drop(addr)
	}
}
