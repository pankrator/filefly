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
	"sort"
	"strings"
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

const replicaFetchTimeout = 5 * time.Second

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
		case "list_files":
			resp = s.listFilesResponse()
		case "delete_file":
			resp = s.deleteFile(req)
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
	totalSize, err := s.determineFileSize(req)
	if err != nil {
		return nil, err
	}
	replicas, err := s.resolveReplicas(req.Replicas)
	if err != nil {
		return nil, err
	}
	meta, err := s.planFileMetadata(name, totalSize, replicas)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.files[name] = *meta
	s.mu.Unlock()
	return meta, nil
}

func (s *Server) planFileMetadata(name string, totalSize, replicas int) (*protocol.FileMetadata, error) {
	if s.blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size")
	}
	blocks, err := s.planBlocks(name, totalSize, replicas)
	if err != nil {
		return nil, err
	}
	meta := protocol.FileMetadata{
		Name:      name,
		TotalSize: totalSize,
		Blocks:    blocks,
		Replicas:  replicas,
	}
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

func (s *Server) planBlocks(fileName string, totalSize, replicas int) ([]protocol.BlockRef, error) {
	if totalSize == 0 {
		return nil, nil
	}
	if replicas <= 0 {
		return nil, fmt.Errorf("replica count must be at least 1")
	}
	if replicas > len(s.dataServers) {
		return nil, fmt.Errorf("replica count %d exceeds available data servers (%d)", replicas, len(s.dataServers))
	}
	blocks := make([]protocol.BlockRef, 0, totalSize/s.blockSize+1)
	remaining := totalSize
	for remaining > 0 {
		chunkSize := s.blockSize
		if chunkSize > remaining {
			chunkSize = remaining
		}
		blockID := fmt.Sprintf("%s-%d", fileName, len(blocks))
		refs := make([]protocol.BlockReplica, 0, replicas)
		for i := 0; i < replicas; i++ {
			refs = append(refs, protocol.BlockReplica{DataServer: s.nextDataServer()})
		}
		blocks = append(blocks, protocol.BlockRef{
			ID:       blockID,
			Size:     chunkSize,
			Replicas: refs,
		})
		remaining -= chunkSize
	}
	return blocks, nil
}

func (s *Server) resolveReplicas(requested int) (int, error) {
	if requested <= 0 {
		requested = 1
	}
	if len(s.dataServers) == 0 {
		return 0, fmt.Errorf("no data servers configured")
	}
	if requested > len(s.dataServers) {
		return 0, fmt.Errorf("replica count %d exceeds available data servers (%d)", requested, len(s.dataServers))
	}
	return requested, nil
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

func (s *Server) listFilesResponse() protocol.MetadataResponse {
	return protocol.MetadataResponse{Status: "ok", Files: s.ListFiles()}
}

func (s *Server) deleteFile(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.FileName == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
	}

	s.mu.RLock()
	meta, ok := s.files[req.FileName]
	s.mu.RUnlock()
	if !ok {
		return protocol.MetadataResponse{Status: "error", Error: "file not found"}
	}

	if err := s.deleteFileBlocks(meta); err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	s.mu.Lock()
	delete(s.files, req.FileName)
	s.mu.Unlock()

	return protocol.MetadataResponse{Status: "ok"}
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
		chunk, err := s.fetchBlockWithFailover(block)
		if err != nil {
			return nil, nil, err
		}
		buf = append(buf, chunk...)
	}
	return buf, &meta, nil
}

func (s *Server) fetchBlockWithFailover(block protocol.BlockRef) ([]byte, error) {
	replicas := normalizeBlockReplicas(block)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("block %s has no replicas", block.ID)
	}
	var lastErr error
	for _, replica := range replicas {
		chunk, err := s.pullBlock(replica.DataServer, block.ID)
		if err == nil {
			return chunk, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("retrieve block %s: %w", block.ID, lastErr)
}

func (s *Server) nextDataServer() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	addr := s.dataServers[s.rrNext%len(s.dataServers)]
	s.rrNext++
	return addr
}

func (s *Server) pullBlock(addr, blockID string) ([]byte, error) {
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

func (s *Server) deleteFileBlocks(meta protocol.FileMetadata) error {
	var errs []string
	for _, block := range meta.Blocks {
		replicas := normalizeBlockReplicas(block)
		for _, replica := range replicas {
			if replica.DataServer == "" {
				continue
			}
			if err := s.sendDelete(replica.DataServer, block.ID); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("delete file %s: %s", meta.Name, strings.Join(errs, "; "))
	}
	return nil
}

func (s *Server) sendDelete(addr, blockID string) error {
	dialer := &net.Dialer{Timeout: replicaFetchTimeout}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(replicaFetchTimeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.DataServerRequest{
		Command: "delete",
		BlockID: blockID,
	}
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
