package dataserver

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
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
	remote := conn.RemoteAddr().String()
	log.Printf("dataserver: accepted connection from %s", remote)
	defer func() {
		log.Printf("dataserver: closed connection from %s", remote)
		conn.Close()
	}()
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
		case "delete":
			resp = s.delete(req)
		case "ping":
			resp = protocol.DataServerResponse{Status: "ok", Pong: true}
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
	checksumPath := s.checksumPath(req.BlockID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("write block: %v", err)}
	}
	sum := crc32.ChecksumIEEE(data)
	if err := writeChecksum(checksumPath, sum); err != nil {
		_ = os.Remove(path)
		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("write checksum: %v", err)}
	}
	log.Printf("dataserver: stored block %s (%d bytes)", req.BlockID, len(data))
	return protocol.DataServerResponse{Status: "ok"}
}

func (s *Server) retrieve(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	path := s.blockPath(req.BlockID)
	checksumPath := s.checksumPath(req.BlockID)
	s.mu.RLock()
	data, err := os.ReadFile(path)
	if err != nil {
		s.mu.RUnlock()
		if os.IsNotExist(err) {
			return protocol.DataServerResponse{Status: "error", Error: "block not found"}
		}
		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("read block: %v", err)}
	}
	storedChecksum, err := readChecksum(checksumPath)
	s.mu.RUnlock()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return protocol.DataServerResponse{Status: "error", Error: "checksum file missing"}
		}
		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("read checksum: %v", err)}
	}
	if crc32.ChecksumIEEE(data) != storedChecksum {
		return protocol.DataServerResponse{Status: "error", Error: "checksum mismatch"}
	}
	log.Printf("dataserver: retrieved block %s (%d bytes)", req.BlockID, len(data))
	return protocol.DataServerResponse{Status: "ok", Data: base64.StdEncoding.EncodeToString(data)}
}

func (s *Server) delete(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	path := s.blockPath(req.BlockID)
	checksumPath := s.checksumPath(req.BlockID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("delete block: %v", err)}
		}
	}
	if err := os.Remove(checksumPath); err != nil {
		if !os.IsNotExist(err) {
			return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("delete checksum: %v", err)}
		}
	}
	log.Printf("dataserver: deleted block %s", req.BlockID)
	return protocol.DataServerResponse{Status: "ok"}
}

func (s *Server) blockPath(blockID string) string {
	safe := base64.RawURLEncoding.EncodeToString([]byte(blockID))
	return filepath.Join(s.storageDir, safe)
}

func (s *Server) checksumPath(blockID string) string {
	return s.blockPath(blockID) + ".crc"
}

func writeChecksum(path string, checksum uint32) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, checksum)
	return os.WriteFile(path, buf, 0o600)
}

func readChecksum(path string) (uint32, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid checksum length: %d", len(data))
	}
	return binary.BigEndian.Uint32(data), nil
}
