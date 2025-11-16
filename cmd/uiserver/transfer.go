package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"golang.org/x/sync/errgroup"

	"filefly/internal/protocol"
)

const replicaFetchTimeout = 5 * time.Second

func uploadBlocks(blocks []protocol.BlockRef, data []byte) error {
	offset := 0
	conns := newConnCache()
	defer conns.CloseAll()
	for _, block := range blocks {
		replicas := normalizeBlockReplicas(block)
		if len(replicas) == 0 {
			return fmt.Errorf("block %s has no replicas to upload", block.ID)
		}
		end := offset + block.Size
		if end > len(data) {
			return fmt.Errorf("block %s exceeds file size", block.ID)
		}
		chunk := data[offset:end]
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

func downloadFile(meta *protocol.FileMetadata, maxConcurrency int) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is nil")
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	blks := meta.Blocks
	chunks := make([][]byte, len(blks))
	sem := make(chan struct{}, maxConcurrency)
	var eg errgroup.Group
	for i := range blks {
		i := i
		block := blks[i]
		eg.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			chunk, err := fetchBlockWithFailover(block)
			if err != nil {
				return err
			}
			chunks[i] = chunk
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, meta.TotalSize)
	for _, chunk := range chunks {
		buf = append(buf, chunk...)
	}
	return buf, nil
}

func fetchBlockWithFailover(block protocol.BlockRef) ([]byte, error) {
	replicas := normalizeBlockReplicas(block)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("block %s has no replicas", block.ID)
	}
	var lastErr error
	for _, replica := range replicas {
		chunk, err := pullBlock(replica.DataServer, block.ID)
		if err == nil {
			return chunk, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("retrieve block %s: %w", block.ID, lastErr)
}

func pullBlock(addr, blockID string) ([]byte, error) {
	dialer := &net.Dialer{Timeout: replicaFetchTimeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(replicaFetchTimeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.DataServerRequest{
		Command: "retrieve",
		BlockID: blockID,
	}
	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("send retrieve to %s: %w", addr, err)
	}
	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("data server %s error: %s", addr, resp.Error)
	}
	data, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 from %s: %w", addr, err)
	}
	return data, nil
}

func normalizeBlockReplicas(block protocol.BlockRef) []protocol.BlockReplica {
	if len(block.Replicas) > 0 {
		return block.Replicas
	}
	if block.DataServer != "" {
		return []protocol.BlockReplica{{DataServer: block.DataServer}}
	}
	return nil
}
