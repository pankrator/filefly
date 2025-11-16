package metadataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"filefly/internal/protocol"
)

// Server keeps metadata for files and distributes blocks to data servers.
type Server struct {
	addr        string
	blockSize   int
	dataServers []string
	persistFreq time.Duration

	planner    *blockPlanner
	store      metadataStore
	persister  *diskPersister
	dataClient *dataServerClient
}

// New creates a metadata server instance.
func New(addr string, blockSize int, dataServers []string, persistPath string, persistFreq time.Duration) *Server {
	selector := newRoundRobinSelector(dataServers)
	return &Server{
		addr:        addr,
		blockSize:   blockSize,
		dataServers: append([]string(nil), dataServers...),
		persistFreq: persistFreq,
		planner:     newBlockPlanner(blockSize, selector),
		store:       newMetadataStore(),
		persister:   newDiskPersister(persistPath),
		dataClient:  newDataServerClient(replicaFetchTimeout),
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
	if len(s.dataServers) == 0 {
		return fmt.Errorf("metadata server requires at least one data server")
	}
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
	meta, err := s.planner.Plan(name, totalSize, req.Replicas)
	if err != nil {
		return nil, err
	}
	if err := s.store.Save(*meta); err != nil {
		return nil, err
	}
	return meta, nil
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
