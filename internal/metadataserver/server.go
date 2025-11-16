package metadataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"filefly/internal/protocol"
)

// Server keeps metadata for files and distributes blocks to data servers.
type Server struct {
	addr        string
	blockSize   int
	dataServers []string
	persistPath string
	persistFreq time.Duration

	mu     sync.RWMutex
	files  map[string]protocol.FileMetadata
	rrNext int
}

type diskSnapshot struct {
	Files map[string]protocol.FileMetadata `json:"files"`
}

// New creates a metadata server instance.
func New(addr string, blockSize int, dataServers []string, persistPath string, persistFreq time.Duration) *Server {
	return &Server{
		addr:        addr,
		blockSize:   blockSize,
		dataServers: append([]string(nil), dataServers...),
		persistPath: persistPath,
		persistFreq: persistFreq,
		files:       make(map[string]protocol.FileMetadata),
	}
}

// Listen starts the server.
func (s *Server) Listen() error {
	if len(s.dataServers) == 0 {
		return fmt.Errorf("metadata server requires at least one data server")
	}
	if err := s.loadFromDisk(); err != nil {
		return fmt.Errorf("metadata bootstrap: %w", err)
	}
	s.startPersistenceLoop()
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
	if s.blockSize <= 0 {
		return protocol.MetadataResponse{Status: "error", Error: "invalid block size"}
	}

	totalSize, err := s.determineFileSize(req)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	blocks := s.planBlocks(req.FileName, totalSize)
	meta := protocol.FileMetadata{
		Name:      req.FileName,
		TotalSize: totalSize,
		Blocks:    blocks,
	}

	s.mu.Lock()
	s.files[req.FileName] = meta
	s.mu.Unlock()

	return protocol.MetadataResponse{Status: "ok", Metadata: &meta}
}

func (s *Server) determineFileSize(req protocol.MetadataRequest) (int, error) {
	if req.FileSize > 0 {
		return req.FileSize, nil
	}
	if req.Data != "" {
		raw, err := base64.StdEncoding.DecodeString(req.Data)
		if err != nil {
			return 0, fmt.Errorf("invalid base64 data")
		}
		return len(raw), nil
	}
	return 0, fmt.Errorf("missing file_size or data")
}

func (s *Server) startPersistenceLoop() {
	if s.persistPath == "" || s.persistFreq <= 0 {
		return
	}
	if err := s.persistToDisk(); err != nil {
		log.Printf("metadata persistence error: %v", err)
	}
	go func() {
		ticker := time.NewTicker(s.persistFreq)
		defer ticker.Stop()
		for range ticker.C {
			if err := s.persistToDisk(); err != nil {
				log.Printf("metadata persistence error: %v", err)
			}
		}
	}()
}

func (s *Server) persistToDisk() error {
	if s.persistPath == "" {
		return nil
	}
	s.mu.RLock()
	snapshot := make(map[string]protocol.FileMetadata, len(s.files))
	for name, meta := range s.files {
		snapshot[name] = meta
	}
	s.mu.RUnlock()

	bytes, err := json.MarshalIndent(diskSnapshot{Files: snapshot}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata snapshot: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.persistPath), 0o755); err != nil {
		return fmt.Errorf("ensure metadata directory: %w", err)
	}
	tmp := s.persistPath + ".tmp"
	if err := os.WriteFile(tmp, bytes, 0o600); err != nil {
		return fmt.Errorf("write temp metadata file: %w", err)
	}
	if err := os.Rename(tmp, s.persistPath); err != nil {
		return fmt.Errorf("replace metadata file: %w", err)
	}
	return nil
}

func (s *Server) loadFromDisk() error {
	if s.persistPath == "" {
		return nil
	}
	f, err := os.Open(s.persistPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open metadata file: %w", err)
	}
	defer f.Close()

	var snapshot diskSnapshot
	dec := json.NewDecoder(f)
	if err := dec.Decode(&snapshot); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("decode metadata file: %w", err)
	}
	if snapshot.Files == nil {
		snapshot.Files = make(map[string]protocol.FileMetadata)
	}
	s.mu.Lock()
	s.files = snapshot.Files
	s.mu.Unlock()
	log.Printf("metadata server loaded %d entries from %s", len(snapshot.Files), s.persistPath)
	return nil
}

func (s *Server) planBlocks(fileName string, totalSize int) []protocol.BlockRef {
	if totalSize == 0 {
		return nil
	}
	blocks := make([]protocol.BlockRef, 0, totalSize/s.blockSize+1)
	remaining := totalSize
	for remaining > 0 {
		chunkSize := s.blockSize
		if chunkSize > remaining {
			chunkSize = remaining
		}
		blockID := fmt.Sprintf("%s-%d", fileName, len(blocks))
		blocks = append(blocks, protocol.BlockRef{
			ID:         blockID,
			DataServer: s.nextDataServer(),
			Size:       chunkSize,
		})
		remaining -= chunkSize
	}
	return blocks
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
