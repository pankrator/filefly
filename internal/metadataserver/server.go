package metadataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
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
	meta, err := s.planAndSaveFile(req.FileName, req)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}
	return protocol.MetadataResponse{Status: "ok", Metadata: meta}
}

func (s *Server) planAndSaveFile(name string, req protocol.MetadataRequest) (*protocol.FileMetadata, error) {
	if s.blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size")
	}
	totalSize, err := s.determineFileSize(req)
	if err != nil {
		return nil, err
	}
	blocks := s.planBlocks(name, totalSize)
	meta := protocol.FileMetadata{
		Name:      name,
		TotalSize: totalSize,
		Blocks:    blocks,
	}
	s.mu.Lock()
	s.files[name] = meta
	s.mu.Unlock()
	return &meta, nil
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
	data, meta, err := s.FetchFileBytes(req.FileName)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}
	return protocol.MetadataResponse{Status: "ok", Data: base64.StdEncoding.EncodeToString(data), Metadata: meta}
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

// ListFiles returns all known metadata entries sorted by name.
func (s *Server) ListFiles() []protocol.FileMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	files := make([]protocol.FileMetadata, 0, len(s.files))
	for _, meta := range s.files {
		files = append(files, meta)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name < files[j].Name
	})
	return files
}

// StoreFileBytes stores a file and uploads its blocks to the data servers.
func (s *Server) StoreFileBytes(name string, data []byte) (*protocol.FileMetadata, error) {
	if name == "" {
		return nil, fmt.Errorf("missing file name")
	}
	req := protocol.MetadataRequest{FileName: name, FileSize: len(data)}
	meta, err := s.planAndSaveFile(name, req)
	if err != nil {
		return nil, err
	}
	if err := s.uploadBlocks(meta.Blocks, data); err != nil {
		return nil, err
	}
	return meta, nil
}

// FetchFileBytes downloads a full file from the data servers.
func (s *Server) FetchFileBytes(name string) ([]byte, *protocol.FileMetadata, error) {
	s.mu.RLock()
	meta, ok := s.files[name]
	s.mu.RUnlock()
	if !ok {
		return nil, nil, fmt.Errorf("file not found")
	}
	buf := make([]byte, 0, meta.TotalSize)
	for _, block := range meta.Blocks {
		data, err := s.pullBlock(block.DataServer, block.ID)
		if err != nil {
			return nil, nil, err
		}
		buf = append(buf, data...)
	}
	return buf, &meta, nil
}

func (s *Server) uploadBlocks(blocks []protocol.BlockRef, data []byte) error {
	offset := 0
	for _, block := range blocks {
		end := offset + block.Size
		if end > len(data) {
			return fmt.Errorf("block %s exceeds file size", block.ID)
		}
		chunk := data[offset:end]
		if err := s.pushBlock(block, chunk); err != nil {
			return err
		}
		offset = end
	}
	if offset != len(data) {
		return fmt.Errorf("plan left %d bytes unused", len(data)-offset)
	}
	return nil
}

func (s *Server) pushBlock(block protocol.BlockRef, data []byte) error {
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
