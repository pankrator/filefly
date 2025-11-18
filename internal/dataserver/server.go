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
	"sync"
	"time"

	"filefly/internal/protocol"
)

type Server struct {
	addr       string
	storageDir string
	mu         sync.RWMutex

	verifyInterval time.Duration
	verifier       *blockVerifier
}

// Option customizes the data server.
type Option func(*Server)

const (
	defaultVerifyInterval = 5 * time.Minute
	peerRepairTimeout     = 5 * time.Second
)

// WithVerificationInterval adjusts how often the background verifier scans stored blocks.
// A zero or negative interval disables the background worker.
func WithVerificationInterval(interval time.Duration) Option {
	return func(s *Server) {
		s.verifyInterval = interval
	}
}

// New creates a new data server listening on the provided address and storing
// block files under the provided directory.
func New(addr, storageDir string, opts ...Option) (*Server, error) {
	if storageDir == "" {
		return nil, fmt.Errorf("storage directory is required")
	}

	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	srv := &Server{
		addr:           addr,
		storageDir:     storageDir,
		verifyInterval: defaultVerifyInterval,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(srv)
		}
	}

	srv.verifier = newBlockVerifier(srv, srv.verifyInterval)

	return srv, nil
}

// Listen starts the TCP server and blocks until the listener fails.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("dataserver listen: %w", err)
	}

	defer ln.Close() //nolint:errcheck

	log.Printf("data server listening on %s", s.addr)

	if s.verifier != nil {
		s.verifier.Start()
		defer s.verifier.Stop()
	}

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

		conn.Close() //nolint:errcheck
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
		case "verify_block":
			resp = s.verifyBlockCommand(req)
		case "verify_all":
			resp = s.verifyAllCommand()
		case "verification_status":
			resp = s.verificationStatus()
		case "repair_block":
			resp = s.repairBlock(req)
		default:
			resp = protocol.DataServerResponse{Status: "error", Error: "unknown command"}
		}

		if err := enc.Encode(resp); err != nil {
			log.Printf("dataserver encode response: %v", err)
			return
		}
	}
}

func (s *Server) verifyBlockCommand(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	if s.verifier == nil {
		return protocol.DataServerResponse{Status: "error", Error: "verification disabled"}
	}

	result := s.verifier.VerifyBlock(req.BlockID)

	return protocol.DataServerResponse{Status: "ok", Verifications: []protocol.BlockVerification{result}}
}

func (s *Server) verifyAllCommand() protocol.DataServerResponse {
	if s.verifier == nil {
		return protocol.DataServerResponse{Status: "error", Error: "verification disabled"}
	}

	summary, err := s.verifier.VerifyAll()
	if err != nil {
		return protocol.DataServerResponse{Status: "error", Error: err.Error()}
	}

	return protocol.DataServerResponse{Status: "ok", VerificationSummary: summary}
}

func (s *Server) verificationStatus() protocol.DataServerResponse {
	if s.verifier == nil {
		return protocol.DataServerResponse{Status: "error", Error: "verification disabled"}
	}

	summary := s.verifier.Summary()
	if summary == nil {
		return protocol.DataServerResponse{Status: "ok"}
	}

	return protocol.DataServerResponse{Status: "ok", VerificationSummary: summary}
}

func (s *Server) store(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		return protocol.DataServerResponse{Status: "error", Error: "invalid base64 data"}
	}

	if err := s.persistBlock(req.BlockID, data); err != nil {
		return protocol.DataServerResponse{Status: "error", Error: err.Error()}
	}

	log.Printf("dataserver: stored block %s (%d bytes)", req.BlockID, len(data))

	return protocol.DataServerResponse{Status: "ok"}
}

func (s *Server) persistBlock(blockID string, data []byte) error {
	path := s.blockPath(blockID)
	checksumPath := s.checksumPath(blockID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write block: %w", err)
	}

	sum := crc32.ChecksumIEEE(data)
	if err := writeChecksum(checksumPath, sum); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("write checksum: %w", err)
	}

	return nil
}

func (s *Server) repairBlock(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	if req.SourceServer == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing source_server"}
	}

	data, err := s.fetchBlockFromPeer(req.SourceServer, req.BlockID)
	if err != nil {
		return protocol.DataServerResponse{Status: "error", Error: err.Error()}
	}

	if err := s.persistBlock(req.BlockID, data); err != nil {
		return protocol.DataServerResponse{Status: "error", Error: err.Error()}
	}

	if s.verifier != nil {
		go s.verifier.VerifyBlock(req.BlockID)
	}

	log.Printf("dataserver: repaired block %s using %s", req.BlockID, req.SourceServer)

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

		return protocol.DataServerResponse{Status: "error", Error: fmt.Sprintf("delete block: %v", err)}
	}

	log.Printf("dataserver: deleted block %s", req.BlockID)

	return protocol.DataServerResponse{Status: "ok"}
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

func (s *Server) fetchBlockFromPeer(addr, blockID string) ([]byte, error) {
	dialer := &net.Dialer{Timeout: peerRepairTimeout}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to peer %s: %w", addr, err)
	}
	defer conn.Close() //nolint:errcheck

	_ = conn.SetDeadline(time.Now().Add(peerRepairTimeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))

	req := protocol.DataServerRequest{Command: "retrieve", BlockID: blockID}
	if err := enc.Encode(req); err != nil {
		return nil, fmt.Errorf("request block %s from %s: %w", blockID, addr, err)
	}

	var resp protocol.DataServerResponse
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}

	if resp.Status != "ok" {
		msg := resp.Error
		if msg == "" {
			msg = "peer returned error"
		}

		return nil, fmt.Errorf("peer %s: %s", addr, msg)
	}

	data, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		return nil, fmt.Errorf("decode peer data: %w", err)
	}

	return data, nil
}
