package metadataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"filefly/internal/protocol"
)

const replicaFetchTimeout = 5 * time.Second

type dataServerClient struct {
	timeout time.Duration
}

func newDataServerClient(timeout time.Duration) *dataServerClient {
	return &dataServerClient{timeout: timeout}
}

func (c *dataServerClient) FetchBlock(block protocol.BlockRef) ([]byte, error) {
	replicas := normalizeBlockReplicas(block)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("block %s has no replicas", block.ID)
	}
	var lastErr error
	for _, replica := range replicas {
		chunk, err := c.pullBlock(replica.DataServer, block.ID)
		if err == nil {
			return chunk, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("retrieve block %s: %w", block.ID, lastErr)
}

func (c *dataServerClient) DeleteFile(meta protocol.FileMetadata) error {
	var errs []string
	for _, block := range meta.Blocks {
		replicas := normalizeBlockReplicas(block)
		for _, replica := range replicas {
			if replica.DataServer == "" {
				continue
			}
			if err := c.sendDelete(replica.DataServer, block.ID); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("delete file %s: %s", meta.Name, strings.Join(errs, "; "))
	}
	return nil
}

func (c *dataServerClient) pullBlock(addr, blockID string) ([]byte, error) {
	dialer := &net.Dialer{Timeout: c.timeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.timeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.DataServerRequest{Command: "retrieve", BlockID: blockID}
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

func (c *dataServerClient) sendDelete(addr, blockID string) error {
	dialer := &net.Dialer{Timeout: c.timeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.timeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.DataServerRequest{Command: "delete", BlockID: blockID}
	if err := enc.Encode(req); err != nil {
		return fmt.Errorf("send delete to %s: %w", addr, err)
	}
	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp.Status != "ok" {
		if resp.Error == "" {
			resp.Error = "data server returned error"
		}
		return fmt.Errorf("data server %s: %s", addr, resp.Error)
	}
	return nil
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
