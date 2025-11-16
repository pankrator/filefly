package metadataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
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
	persistFreq time.Duration

	selector   *roundRobinSelector
	planner    *blockPlanner
	store      metadataStore
	persister  *diskPersister
	dataClient *dataServerClient
	health     *dataHealthMonitor

	mu sync.RWMutex
}

// New creates a metadata server instance.
func New(addr string, blockSize int, dataServers []string, persistPath string, persistFreq time.Duration) *Server {
	selector := newRoundRobinSelector(dataServers)
	client := newDataServerClient(replicaFetchTimeout)
	return &Server{
		addr:        addr,
		blockSize:   blockSize,
		dataServers: append([]string(nil), dataServers...),
		persistFreq: persistFreq,
		selector:    selector,
		planner:     newBlockPlanner(blockSize, selector),
		store:       newMetadataStore(),
		persister:   newDiskPersister(persistPath),
		dataClient:  client,
		health:      newDataHealthMonitor(dataServers, client, defaultHealthCheckInterval),
	}
}

// Listen starts the server.
func (s *Server) Listen() error {
	if err := s.validateConfig(); err != nil {
		return err
	}
	if err := s.persister.Load(s.store); err != nil {
		return fmt.Errorf("metadata bootstrap: %w", err)
	}
	s.persister.Start(s.store, s.persistFreq)
	if s.health != nil {
		s.health.Start()
		defer s.health.Stop()
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

func (s *Server) validateConfig() error {
	if s.blockSize <= 0 {
		return fmt.Errorf("metadata server requires a positive block size")
	}
	return nil
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
		case "complete_file":
			resp = s.completeFile(req)
		case "fetch_file":
			resp = s.fetchFile(req)
		case "get_metadata":
			resp = s.getMetadata(req)
		case "list_files":
			resp = s.listFilesResponse()
		case "list_data_servers":
			resp = s.listDataServersResponse()
		case "delete_file":
			resp = s.deleteFile(req)
		case "ping":
			resp = protocol.MetadataResponse{Status: "ok"}
		case "register_data_server":
			resp = s.registerDataServer(req)
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
	meta, err := s.planFile(req.FileName, req)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}
	return protocol.MetadataResponse{Status: "ok", Metadata: meta}
}

func (s *Server) planFile(name string, req protocol.MetadataRequest) (*protocol.FileMetadata, error) {
	totalSize, err := s.determineFileSize(req)
	if err != nil {
		return nil, err
	}
	meta, err := s.planner.Plan(name, totalSize, req.Replicas)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Server) completeFile(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.Metadata == nil {
		return protocol.MetadataResponse{Status: "error", Error: "missing metadata"}
	}
	meta := *req.Metadata
	if meta.Name == "" {
		if req.FileName != "" {
			meta.Name = req.FileName
		} else {
			return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
		}
	}
	if err := validateMetadata(meta); err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}
	if err := s.store.Save(meta); err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}
	return protocol.MetadataResponse{Status: "ok", Metadata: &meta}
}

func validateMetadata(meta protocol.FileMetadata) error {
	if meta.Name == "" {
		return fmt.Errorf("metadata missing name")
	}
	if meta.TotalSize < 0 {
		return fmt.Errorf("metadata total_size cannot be negative")
	}
	var total int
	for _, block := range meta.Blocks {
		if block.ID == "" {
			return fmt.Errorf("metadata block missing id")
		}
		if block.Size < 0 {
			return fmt.Errorf("metadata block %s has negative size", block.ID)
		}
		replicas := len(normalizeBlockReplicas(block))
		if block.Size > 0 && replicas == 0 {
			return fmt.Errorf("metadata block %s has no replicas", block.ID)
		}
		if block.Size > 0 && meta.Replicas > 0 && replicas != meta.Replicas {
			return fmt.Errorf("metadata block %s replica mismatch: have %d want %d", block.ID, replicas, meta.Replicas)
		}
		total += block.Size
	}
	if total != meta.TotalSize {
		return fmt.Errorf("metadata size mismatch: blocks total %d but metadata reports %d", total, meta.TotalSize)
	}
	return nil
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
	meta, ok := s.store.Get(req.FileName)
	if !ok {
		return protocol.MetadataResponse{Status: "error", Error: "file not found"}
	}
	copy := meta
	return protocol.MetadataResponse{Status: "ok", Metadata: &copy}
}

func (s *Server) listFilesResponse() protocol.MetadataResponse {
	return protocol.MetadataResponse{Status: "ok", Files: s.store.List()}
}

func (s *Server) deleteFile(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.FileName == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
	}

	meta, err := s.store.BeginDelete(req.FileName)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	if err := s.dataClient.DeleteFile(meta); err != nil {
		s.store.CancelDelete(req.FileName)
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	s.store.CompleteDelete(req.FileName)
	return protocol.MetadataResponse{Status: "ok"}
}

// ListFiles returns all known metadata entries sorted by name.
func (s *Server) ListFiles() []protocol.FileMetadata {
	return s.store.List()
}

// ListDataServers returns the latest health information for data servers.
func (s *Server) ListDataServers() []protocol.DataServerHealth {
	if s.health == nil {
		return nil
	}
	return s.health.List()
}

func (s *Server) registerDataServer(req protocol.MetadataRequest) protocol.MetadataResponse {
	addr := strings.TrimSpace(req.DataServerAddr)
	if addr == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing data_server_addr"}
	}
	added := s.ensureDataServer(addr)
	if s.health != nil {
		s.health.CheckNow(addr)
	}
	if added {
		log.Printf("metadata: registered data server %s", addr)
	}
	return protocol.MetadataResponse{Status: "ok"}
}

func (s *Server) ensureDataServer(addr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.dataServers {
		if existing == addr {
			if s.health != nil {
				s.health.EnsureServer(addr)
			}
			return false
		}
	}
	s.dataServers = append(s.dataServers, addr)
	if s.selector != nil {
		s.selector.SetServers(s.dataServers)
	}
	if s.health != nil {
		s.health.EnsureServer(addr)
	}
	return true
}

// FetchFileBytes downloads a full file from the data servers.
func (s *Server) FetchFileBytes(name string) ([]byte, *protocol.FileMetadata, error) {
	meta, ok := s.store.Get(name)
	if !ok {
		return nil, nil, fmt.Errorf("file not found")
	}
	buf := make([]byte, 0, meta.TotalSize)
	for _, block := range meta.Blocks {
		chunk, err := s.dataClient.FetchBlock(block)
		if err != nil {
			return nil, nil, err
		}
		buf = append(buf, chunk...)
	}
	copy := meta
	return buf, &copy, nil
}

func (s *Server) listDataServersResponse() protocol.MetadataResponse {
	servers := s.ListDataServers()
	if servers == nil {
		servers = []protocol.DataServerHealth{}
	}
	return protocol.MetadataResponse{Status: "ok", Servers: servers}
}
