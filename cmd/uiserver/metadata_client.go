package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"filefly/internal/protocol"
)

var errNotFound = errors.New("file not found")

type metadataClient struct {
	addr string
}

func newMetadataClient(addr string) *metadataClient {
	return &metadataClient{addr: addr}
}

func (c *metadataClient) listFiles() ([]protocol.FileMetadata, error) {
	resp, err := c.request(protocol.MetadataRequest{Command: "list_files"})
	if err != nil {
		return nil, err
	}
	return resp.Files, nil
}

func (c *metadataClient) planFile(name string, size, replicas int) (*protocol.FileMetadata, error) {
	req := protocol.MetadataRequest{
		Command:  "store_file",
		FileName: name,
		FileSize: size,
		Replicas: replicas,
	}
	resp, err := c.request(req)
	if err != nil {
		return nil, err
	}
	if resp.Metadata == nil {
		return nil, fmt.Errorf("metadata server returned no plan for %s", name)
	}
	return resp.Metadata, nil
}

func (c *metadataClient) completeFile(meta *protocol.FileMetadata) error {
	if meta == nil {
		return fmt.Errorf("nil metadata")
	}
	_, err := c.request(protocol.MetadataRequest{Command: "complete_file", Metadata: meta})
	return err
}

func (c *metadataClient) getMetadata(name string) (*protocol.FileMetadata, error) {
	resp, err := c.request(protocol.MetadataRequest{Command: "get_metadata", FileName: name})
	if err != nil {
		return nil, err
	}
	if resp.Metadata == nil {
		return nil, fmt.Errorf("metadata server returned no metadata for %s", name)
	}
	return resp.Metadata, nil
}

func (c *metadataClient) deleteFile(name string) error {
	_, err := c.request(protocol.MetadataRequest{Command: "delete_file", FileName: name})
	return err
}

func (c *metadataClient) request(req protocol.MetadataRequest) (*protocol.MetadataResponse, error) {
	conn, err := net.Dial("tcp", c.addr)
	if err != nil {
		return nil, fmt.Errorf("connect to metadata server: %w", err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("send %s: %w", req.Command, err)
	}

	var resp protocol.MetadataResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if resp.Status != "ok" {
		if resp.Error == "file not found" {
			return nil, errNotFound
		}
		if resp.Error == "" {
			resp.Error = "metadata server returned error"
		}
		return nil, errors.New(resp.Error)
	}
	return &resp, nil
}
