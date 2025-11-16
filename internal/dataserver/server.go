package dataserver

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

// Server keeps blocks in memory. It intentionally keeps everything in process
// memory so it is easy to reason about for demos and unit tests.
type Server struct {
	addr   string
	blocks map[string][]byte
	mu     sync.RWMutex
}

// New creates a new data server listening on the provided address.
func New(addr string) *Server {
	return &Server{
		addr:   addr,
		blocks: make(map[string][]byte),
	}
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

	s.mu.Lock()
	s.blocks[req.BlockID] = data
	s.mu.Unlock()

	return protocol.DataServerResponse{Status: "ok"}
}

func (s *Server) retrieve(req protocol.DataServerRequest) protocol.DataServerResponse {
	if req.BlockID == "" {
		return protocol.DataServerResponse{Status: "error", Error: "missing block_id"}
	}

	s.mu.RLock()
	data, ok := s.blocks[req.BlockID]
	s.mu.RUnlock()
	if !ok {
		return protocol.DataServerResponse{Status: "error", Error: "block not found"}
	}

	return protocol.DataServerResponse{Status: "ok", Data: base64.StdEncoding.EncodeToString(data)}
}
