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
	resp, err := c.request(addr, protocol.DataServerRequest{Command: "retrieve", BlockID: blockID})
	if err != nil {
		return nil, err
	}

	if resp.Status != "ok" {
		return nil, fmt.Errorf("data server %s error: %s", addr, messageOrDefault(resp.Error, "retrieve failed"))
	}

	data, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 from %s: %w", addr, err)
	}

	return data, nil
}

func (c *dataServerClient) sendDelete(addr, blockID string) error {
	resp, err := c.request(addr, protocol.DataServerRequest{Command: "delete", BlockID: blockID})
	if err != nil {
		return err
	}

	if resp.Status != "ok" {
		return fmt.Errorf("data server %s: %s", addr, messageOrDefault(resp.Error, "delete failed"))
	}

	return nil
}

func (c *dataServerClient) Ping(addr string) error {
	resp, err := c.request(addr, protocol.DataServerRequest{Command: "ping"})
	if err != nil {
		return err
	}

	if resp.Status != "ok" || !resp.Pong {
		return fmt.Errorf("data server %s: %s", addr, messageOrDefault(resp.Error, "no pong received"))
	}

	return nil
}

func (c *dataServerClient) request(addr string, req protocol.DataServerRequest) (*protocol.DataServerResponse, error) {
	dialer := &net.Dialer{Timeout: c.timeout}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to data server %s: %w", addr, err)
	}

	defer conn.Close() //nolint:errcheck

	_ = conn.SetDeadline(time.Now().Add(c.timeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))

	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("send %s to %s: %w", req.Command, addr, err)
	}

	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}

	return &resp, nil
}

func messageOrDefault(message, fallback string) string {
	if message != "" {
		return message
	}

	return fallback
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
