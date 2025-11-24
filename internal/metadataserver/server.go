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
	addr                 string
	blockSize            int
	dataServers          []string
	advertised           map[string]string
	internal             map[string]string
	persistFreq          time.Duration
	configuredAdvertised []string

	selector   *roundRobinSelector
	planner    *blockPlanner
	store      metadataStore
	persister  *diskPersister
	dataClient *dataServerClient
	health     *dataHealthMonitor
	integrity  *dataIntegrityAuditor

	mu sync.RWMutex
}

// New creates a metadata server instance.
func New(
	addr string,
	blockSize int,
	dataServers []string,
	advertisedDataServers []string,
	persistPath string,
	persistFreq time.Duration,
	integrityInterval time.Duration,
) (*Server, error) {
	selector := newRoundRobinSelector(dataServers)
	client := newDataServerClient(replicaFetchTimeout)

	advertised, internal, err := buildAdvertisedMappings(dataServers, advertisedDataServers)
	if err != nil {
		return nil, err
	}

	srv := &Server{
		addr:                 addr,
		blockSize:            blockSize,
		dataServers:          append([]string(nil), dataServers...),
		advertised:           advertised,
		internal:             internal,
		persistFreq:          persistFreq,
		configuredAdvertised: append([]string(nil), advertisedDataServers...),
		selector:             selector,
		planner:              newBlockPlanner(blockSize, selector),
		store:                newMetadataStore(),
		persister:            newDiskPersister(persistPath),
		dataClient:           client,
		health:               newDataHealthMonitor(dataServers, client, defaultHealthCheckInterval),
	}

	srv.integrity = newDataIntegrityAuditor(srv, integrityInterval)

	return srv, nil
}

func buildAdvertisedMappings(
	dataServers []string,
	advertisedDataServers []string,
) (map[string]string, map[string]string, error) {
	if len(advertisedDataServers) > 0 && len(advertisedDataServers) != len(dataServers) {
		return nil, nil, fmt.Errorf(
			"metadata server requires the same number of advertised data servers as internal data servers (have %d, want %d)",
			len(advertisedDataServers),
			len(dataServers),
		)
	}

	advertised := make(map[string]string, len(dataServers))
	internal := make(map[string]string, len(dataServers))

	for i, addr := range dataServers {
		advertisedAddr := addr

		if len(advertisedDataServers) > 0 && advertisedDataServers[i] != "" {
			advertisedAddr = advertisedDataServers[i]
		}

		advertised[addr] = advertisedAddr
		internal[advertisedAddr] = addr
	}

	return advertised, internal, nil
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

	if s.integrity != nil {
		s.integrity.Start()
		defer s.integrity.Stop()
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metadata server listen: %w", err)
	}

	defer ln.Close() //nolint:errcheck

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

//nolint:funlen
func (s *Server) handleConn(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	log.Printf("metadata: accepted connection from %s", remote)

	defer func() {
		log.Printf("metadata: closed connection from %s", remote)

		err := conn.Close()
		if err != nil {
			log.Printf("metadata: close connection from %s: %v", remote, err)
		}
	}()

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
		case "verify_data_server":
			resp = s.verifyDataServer(req)
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

	log.Printf("metadata: planned upload for %s (%d bytes across %d blocks, replicas=%d)",
		meta.Name,
		meta.TotalSize,
		len(meta.Blocks),
		meta.Replicas)

	return protocol.MetadataResponse{Status: "ok", Metadata: s.advertiseMetadata(meta)}
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

	meta := s.internalizeMetadata(*req.Metadata)
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

	log.Printf("metadata: stored metadata for %s (%d blocks)", meta.Name, len(meta.Blocks))

	return protocol.MetadataResponse{Status: "ok", Metadata: s.advertiseMetadata(&meta)}
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

	log.Printf("metadata: fetching %s for caller", req.FileName)

	data, meta, err := s.FetchFileBytes(req.FileName)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	return protocol.MetadataResponse{
		Status:   "ok",
		Data:     base64.StdEncoding.EncodeToString(data),
		Metadata: s.advertiseMetadata(meta),
	}
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

	return protocol.MetadataResponse{Status: "ok", Metadata: s.advertiseMetadata(&copy)}
}

func (s *Server) listFilesResponse() protocol.MetadataResponse {
	return protocol.MetadataResponse{Status: "ok", Files: s.advertiseFiles(s.store.List())}
}

func (s *Server) deleteFile(req protocol.MetadataRequest) protocol.MetadataResponse {
	if req.FileName == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing file_name"}
	}

	log.Printf("metadata: deleting %s", req.FileName)

	meta, err := s.store.BeginDelete(req.FileName)
	if err != nil {
		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	if err := s.dataClient.DeleteFile(meta); err != nil {
		s.store.CancelDelete(req.FileName)
		log.Printf("metadata: delete %s failed, rolled back metadata: %v", req.FileName, err)

		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	s.store.CompleteDelete(req.FileName)
	log.Printf("metadata: deleted %s", req.FileName)

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

	advertised := strings.TrimSpace(req.AdvertisedAddr)

	added := s.ensureDataServer(addr, advertised)

	if s.health != nil {
		s.health.CheckNow(addr)
	}

	if added {
		log.Printf("metadata: registered data server %s", addr)
	}

	return protocol.MetadataResponse{Status: "ok"}
}

func (s *Server) verifyDataServer(req protocol.MetadataRequest) protocol.MetadataResponse {
	addr := s.internalAddress(strings.TrimSpace(req.DataServerAddr))
	if addr == "" {
		return protocol.MetadataResponse{Status: "error", Error: "missing data_server_addr"}
	}

	if !s.isKnownDataServer(addr) {
		return protocol.MetadataResponse{Status: "error", Error: "unknown data_server_addr"}
	}

	if s.dataClient == nil {
		return protocol.MetadataResponse{Status: "error", Error: "metadata server is not configured with a data client"}
	}

	summary, err := s.dataClient.VerifyAll(addr)
	if err != nil {
		s.recordVerification(addr, nil, err)

		return protocol.MetadataResponse{Status: "error", Error: err.Error()}
	}

	s.recordVerification(addr, summary, nil)

	health := protocol.DataServerHealth{Address: addr, Verification: summary}

	return protocol.MetadataResponse{Status: "ok", Servers: []protocol.DataServerHealth{health}}
}

func (s *Server) ensureDataServer(internalAddr, advertisedAddr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setAdvertisedMapping(internalAddr, advertisedAddr)

	for _, existing := range s.dataServers {
		if existing == internalAddr {
			if s.health != nil {
				s.health.EnsureServer(internalAddr)
			}

			return false
		}
	}

	s.dataServers = append(s.dataServers, internalAddr)
	if s.selector != nil {
		s.selector.SetServers(s.dataServers)
	}

	if s.health != nil {
		s.health.EnsureServer(internalAddr)
	}

	return true
}

func (s *Server) isKnownDataServer(addr string) bool {
	internalAddr := s.internalAddress(addr)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, existing := range s.dataServers {
		if existing == internalAddr {
			return true
		}
	}

	return false
}

func (s *Server) dataServersSnapshot() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return append([]string(nil), s.dataServers...)
}

func (s *Server) setAdvertisedMapping(internalAddr, advertisedAddr string) {
	if internalAddr == "" {
		return
	}

	if advertisedAddr == "" {
		advertisedAddr = internalAddr
	}

	if s.advertised == nil {
		s.advertised = make(map[string]string)
	}

	if s.internal == nil {
		s.internal = make(map[string]string)
	}

	if existing, ok := s.advertised[internalAddr]; ok && existing != advertisedAddr {
		delete(s.internal, existing)
	}

	s.advertised[internalAddr] = advertisedAddr
	s.internal[advertisedAddr] = internalAddr
}

func (s *Server) advertisedAddress(internalAddr string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if advertised, ok := s.advertised[internalAddr]; ok && advertised != "" {
		return advertised
	}

	return internalAddr
}

func (s *Server) internalAddress(advertisedAddr string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if internalAddr, ok := s.internal[advertisedAddr]; ok && internalAddr != "" {
		return internalAddr
	}

	return advertisedAddr
}

func (s *Server) recordVerification(addr string, summary *protocol.BlockVerificationSummary, err error) {
	if s.health == nil {
		return
	}

	s.health.RecordVerification(addr, summary, err)
}

func (s *Server) advertiseMetadata(meta *protocol.FileMetadata) *protocol.FileMetadata {
	if meta == nil {
		return nil
	}

	copy := *meta
	copy.Blocks = make([]protocol.BlockRef, len(meta.Blocks))

	for i, block := range meta.Blocks {
		copy.Blocks[i] = s.advertiseBlock(block)
	}

	return &copy
}

func (s *Server) internalizeMetadata(meta protocol.FileMetadata) protocol.FileMetadata {
	internal := meta
	internal.Blocks = make([]protocol.BlockRef, len(meta.Blocks))

	for i, block := range meta.Blocks {
		internal.Blocks[i] = s.internalizeBlock(block)
	}

	return internal
}

func (s *Server) advertiseBlock(block protocol.BlockRef) protocol.BlockRef {
	advertised := block

	if block.Replicas != nil {
		advertised.Replicas = make([]protocol.BlockReplica, len(block.Replicas))
		for i, replica := range block.Replicas {
			advertised.Replicas[i] = protocol.BlockReplica{DataServer: s.advertisedAddress(replica.DataServer)}
		}
	}

	if block.DataServer != "" {
		advertised.DataServer = s.advertisedAddress(block.DataServer)
	}

	return advertised
}

func (s *Server) internalizeBlock(block protocol.BlockRef) protocol.BlockRef {
	internal := block

	if block.Replicas != nil {
		internal.Replicas = make([]protocol.BlockReplica, len(block.Replicas))
		for i, replica := range block.Replicas {
			internal.Replicas[i] = protocol.BlockReplica{DataServer: s.internalAddress(replica.DataServer)}
		}
	}

	if block.DataServer != "" {
		internal.DataServer = s.internalAddress(block.DataServer)
	}

	return internal
}

func (s *Server) advertiseFiles(files []protocol.FileMetadata) []protocol.FileMetadata {
	copies := make([]protocol.FileMetadata, 0, len(files))
	for i := range files {
		copies = append(copies, *s.advertiseMetadata(&files[i]))
	}

	return copies
}

func (s *Server) advertiseHealth(servers []protocol.DataServerHealth) []protocol.DataServerHealth {
	advertised := make([]protocol.DataServerHealth, len(servers))

	for i, server := range servers {
		advertised[i] = server
		advertised[i].Address = s.advertisedAddress(server.Address)
	}

	return advertised
}

func (s *Server) repairReplica(targetAddr, blockID string) error {
	if s.store == nil {
		return fmt.Errorf("metadata store not configured")
	}

	if s.dataClient == nil {
		return fmt.Errorf("metadata server has no data client")
	}

	meta, block, ok := s.store.FindBlock(blockID)
	if !ok {
		return fmt.Errorf("block %s is unknown to metadata store", blockID)
	}

	source := s.selectRepairSource(block, targetAddr)
	if source == "" {
		return fmt.Errorf("no alternate replica available for %s", blockID)
	}

	if err := s.dataClient.RepairBlock(targetAddr, blockID, source); err != nil {
		return fmt.Errorf("repair %s on %s: %w", blockID, targetAddr, err)
	}

	log.Printf("metadata: instructed %s to repair block %s (%s) using %s", targetAddr, blockID, meta.Name, source)

	return nil
}

func (s *Server) selectRepairSource(block protocol.BlockRef, target string) string {
	replicas := normalizeBlockReplicas(block)
	for _, replica := range replicas {
		if replica.DataServer == "" || replica.DataServer == target {
			continue
		}

		return replica.DataServer
	}

	if block.DataServer != "" && block.DataServer != target {
		return block.DataServer
	}

	return ""
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

	s.attachVerification(servers)

	return protocol.MetadataResponse{Status: "ok", Servers: s.advertiseHealth(servers)}
}

func (s *Server) attachVerification(servers []protocol.DataServerHealth) {
	if s == nil || s.dataClient == nil {
		return
	}

	for i := range servers {
		addr := servers[i].Address
		if addr == "" {
			continue
		}

		summary, err := s.dataClient.VerificationStatus(addr)
		if err != nil {
			servers[i].VerificationError = err.Error()
			continue
		}

		servers[i].VerificationError = ""
		servers[i].Verification = summary
	}
}
