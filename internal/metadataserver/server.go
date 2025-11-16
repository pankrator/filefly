package metadataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"filefly/internal/protocol"
)

// Server keeps metadata for files and distributes blocks to data servers.
type Server struct {
	addr        string
	blockSize   int
	dataServers []string

	mu     sync.RWMutex
	files  map[string]protocol.FileMetadata
	rrNext int
}

// New creates a metadata server instance.
func New(addr string, blockSize int, dataServers []string) *Server {
	return &Server{
		addr:        addr,
		blockSize:   blockSize,
		dataServers: append([]string(nil), dataServers...),
		files:       make(map[string]protocol.FileMetadata),
	}
}

// Listen starts the server.
func (s *Server) Listen() error {
	if len(s.dataServers) == 0 {
		return fmt.Errorf("metadata server requires at least one data server")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metadata server listen: %w", err)
	}
	defer ln.Close()

	log.Printf("metadata server listening on %s (block size=%d)", s.addr, s.blockSize)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("metadata accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(bufio.NewReader(conn))
	enc := json.NewEncoder(conn)

	for {
		var req protocol.MetadataRequest
		if err := dec.Decode(&req); err != nil {
			if err == io.EOF {
				return
			}
			_ = enc.Encode(protocol.MetadataResponse{Status: "error", Error: err.Error()})
			return
		}

		var resp protocol.MetadataResponse
		switch req.Command {
		case "store_file":
			resp = s.storeFile(req)
		case "fetch_file":
			resp = s.fetchFile(req)
		case "get_metadata":
			resp = s.getMetadata(req)
		case "ping":
			resp = protocol.MetadataResponse{Status: "ok"}
		default:
			resp = protocol.MetadataResponse{Status: "error", Error: "unknown command"}
		}

		if err := enc.Encode(resp); err != nil {
			log.Printf("metadata encode response: %v", err)
			return
		}
	}
}

func (s *Server) storeFile(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.FileName == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
	}
	raw, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: "invalid base64 data"}
	}
	if s.blockSize <= 0 {
		return protocol.MetadataResponse{Status: "error", Error: "invalid block size"}
	}

	blocks := make([]protocol.BlockRef, 0, len(raw)/s.blockSize+1)
	for offset := 0; offset < len(raw); offset += s.blockSize {
		end := offset + s.blockSize
		if end > len(raw) {
			end = len(raw)
		}
		chunk := raw[offset:end]
		blockID := fmt.Sprintf("%s-%d", req.FileName, len(blocks))
		dataServer := s.nextDataServer()
		if err := s.pushBlock(dataServer, blockID, chunk); err != nil {
			return protocol.MetadataResponse{Status: "error", Error: err.Error()}
		}
		blocks = append(blocks, protocol.BlockRef{
			ID:         blockID,
			DataServer: dataServer,
			Size:       len(chunk),
		})
	}

	meta := protocol.FileMetadata{
		Name:      req.FileName,
		TotalSize: len(raw),
		Blocks:    blocks,
	}

	s.mu.Lock()
	s.files[req.FileName] = meta
	s.mu.Unlock()

	return protocol.MetadataResponse{Status: "ok", Metadata: &meta}
}

func (s *Server) fetchFile(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.FileName == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
	}

	s.mu.RLock()
	meta, ok := s.files[req.FileName]
	s.mu.RUnlock()
	if !ok {
		return protocol.MetadataResponse{Status: "error", Error: "file not found"}
	}

	buf := make([]byte, 0, meta.TotalSize)
	for _, block := range meta.Blocks {
		data, err := s.pullBlock(block.DataServer, block.ID)
		if err != nil {
			return protocol.MetadataResponse{Status: "error", Error: err.Error()}
		}
		buf = append(buf, data...)
	}

	return protocol.MetadataResponse{Status: "ok", Data: base64.StdEncoding.EncodeToString(buf), Metadata: &meta}
}

func (s *Server) getMetadata(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.FileName == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
	}

	s.mu.RLock()
	meta, ok := s.files[req.FileName]
	s.mu.RUnlock()
	if !ok {
		return protocol.MetadataResponse{Status: "error", Error: "file not found"}
	}

	return protocol.MetadataResponse{Status: "ok", Metadata: &meta}
}

func (s *Server) nextDataServer() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	addr := s.dataServers[s.rrNext%len(s.dataServers)]
	s.rrNext++
	return addr
}

func (s *Server) pushBlock(addr, blockID string, data []byte) error {
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
		Data:    base64.StdEncoding.EncodeToString(data),
	}
	if err := enc.Encode(req); err != nil {
		return fmt.Errorf("send block to %s: %w", addr, err)
	}
	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp.Status != "ok" {
		return fmt.Errorf("data server %s error: %s", addr, resp.Error)
	}
	return nil
}

func (s *Server) pullBlock(addr, blockID string) ([]byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	defer conn.Close()

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
