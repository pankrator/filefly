package dataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"filefly/internal/protocol"
)

type Server struct {
	addr       string
	storageDir string
	mu         sync.RWMutex
}

// New creates a new data server listening on the provided address and storing
// block files under the provided directory.
func New(addr, storageDir string) (*Server, error) {
	if storageDir == "" {
		return nil, fmt.Errorf("storage directory is required")
	}
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}
	return &Server{
		addr:       addr,
		storageDir: storageDir,
	}, nil
}

// Listen starts the TCP server and blocks until the listener fails.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("dataserver listen: %w", err)
	}
	defer ln.Close()

	log.Printf("data server listening on %s", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("dataserver accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	dec := json.NewDecoder(bufio.NewReader(conn))
	enc := json.NewEncoder(conn)

	for {
		var req protocol.DataServerRequest
		if err := dec.Decode(&req); err != nil {
			if err == io.EOF {
				return
			}
			_ = enc.Encode(protocol.DataServerResponse{Status: "error", Error: err.Error()})
			return
		}

		var resp protocol.DataServerResponse
		switch req.Command {
		case "store":
			resp = s.store(req)
		case "retrieve":
			resp = s.retrieve(req)
		case "ping":
			resp = protocol.DataServerResponse{Status: "ok"}
		default:
			resp = protocol.DataServerResponse{Status: "error", Error: "unknown command"}
		}

		if err := enc.Encode(resp); err != nil {
			log.Printf("dataserver encode response: %v", err)
			return
		}
	}
}

func (s *Server) store(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}
	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		return protocol.DataServerResponse{Status: "error", Error: "invalid base64 data"}
	}

	path := s.blockPath(req.BlockID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("write block: %v", err)}
	}

	return protocol.DataServerResponse{Status: "ok"}
}

func (s *Server) retrieve(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	path := s.blockPath(req.BlockID)
	s.mu.RLock()
	data, err := os.ReadFile(path)
	s.mu.RUnlock()
	if err != nil {
		if os.IsNotExist(err) {
			return protocol.DataServerResponse{Status: "error", Error: "block not found"}
		}
		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("read block: %v", err)}
	}

	return protocol.DataServerResponse{Status: "ok", Data: base64.StdEncoding.EncodeToString(data)}
}

func (s *Server) blockPath(blockID string) string {
	safe := base64.RawURLEncoding.EncodeToString([]byte(blockID))
	return filepath.Join(s.storageDir, safe)
}
