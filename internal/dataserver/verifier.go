package dataserver

import (
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"filefly/internal/protocol"
)

type blockVerifier struct {
	server   *Server
	interval time.Duration

	mu        sync.RWMutex
	lastScan  time.Time
	lastTotal int
	corrupted map[string]protocol.BlockVerification
	stop      chan struct{}
	once      sync.Once
	wg        sync.WaitGroup
}

func newBlockVerifier(server *Server, interval time.Duration) *blockVerifier {
	return &blockVerifier{
		server:    server,
		interval:  interval,
		corrupted: make(map[string]protocol.BlockVerification),
		stop:      make(chan struct{}),
	}
}

func (v *blockVerifier) Start() {
	if v == nil || v.interval <= 0 {
		return
	}

	v.wg.Add(1)
	go v.run()
}

func (v *blockVerifier) Stop() {
	if v == nil || v.interval <= 0 {
		return
	}

	v.once.Do(func() { close(v.stop) })
	v.wg.Wait()
}

func (v *blockVerifier) run() {
	defer v.wg.Done()

	ticker := time.NewTicker(v.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := v.VerifyAll(); err != nil {
				log.Printf("dataserver: background verification failed: %v", err)
			}
		case <-v.stop:
			return
		}
	}
}

func (v *blockVerifier) VerifyAll() (*protocol.BlockVerificationSummary, error) {
	if v == nil {
		return nil, fmt.Errorf("verifier not configured")
	}

	worklist, err := v.server.listBlockEntries()
	if err != nil {
		return nil, err
	}

	unhealthy := make(map[string]protocol.BlockVerification)
	healthyCount := 0

	for _, entry := range worklist {
		var result protocol.BlockVerification

		if !entry.hasBlockFile {
			result = protocol.BlockVerification{
				BlockID:     entry.blockID,
				LastChecked: time.Now().UTC(),
				Error:       "block not found",
			}
		} else {
			result = v.server.verifyBlockIntegrity(entry.blockID)
		}

		if result.Healthy {
			healthyCount++
			continue
		}

		unhealthy[entry.blockID] = result
		log.Printf("dataserver: verification failed for %s: %s", entry.blockID, result.Error)
	}

	summary := &protocol.BlockVerificationSummary{
		TotalBlocks:     len(worklist),
		HealthyBlocks:   healthyCount,
		UnhealthyBlocks: len(unhealthy),
		LastScan:        time.Now().UTC(),
	}

	if len(unhealthy) > 0 {
		summary.CorruptedBlocks = make([]protocol.BlockVerification, 0, len(unhealthy))
		for _, entry := range unhealthy {
			summary.CorruptedBlocks = append(summary.CorruptedBlocks, entry)
		}

		sort.Slice(summary.CorruptedBlocks, func(i, j int) bool {
			return summary.CorruptedBlocks[i].BlockID < summary.CorruptedBlocks[j].BlockID
		})
	}

	v.mu.Lock()
	v.lastScan = summary.LastScan
	v.lastTotal = len(worklist)
	v.corrupted = unhealthy
	v.mu.Unlock()

	return summary, nil
}

func (v *blockVerifier) VerifyBlock(blockID string) protocol.BlockVerification {
	result := v.server.verifyBlockIntegrity(blockID)

	v.mu.Lock()
	if v.corrupted == nil {
		v.corrupted = make(map[string]protocol.BlockVerification)
	}

	if result.Healthy {
		delete(v.corrupted, blockID)
	} else {
		v.corrupted[blockID] = result
		log.Printf("dataserver: verification failed for %s: %s", blockID, result.Error)
	}
	v.mu.Unlock()

	return result
}

func (v *blockVerifier) Summary() *protocol.BlockVerificationSummary {
	if v == nil {
		return nil
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.lastScan.IsZero() && len(v.corrupted) == 0 {
		return nil
	}

	summary := &protocol.BlockVerificationSummary{
		TotalBlocks:     v.lastTotal,
		HealthyBlocks:   v.lastTotal - len(v.corrupted),
		UnhealthyBlocks: len(v.corrupted),
		LastScan:        v.lastScan,
	}

	if summary.HealthyBlocks < 0 {
		summary.HealthyBlocks = 0
	}

	if len(v.corrupted) > 0 {
		summary.CorruptedBlocks = make([]protocol.BlockVerification, 0, len(v.corrupted))
		for _, entry := range v.corrupted {
			summary.CorruptedBlocks = append(summary.CorruptedBlocks, entry)
		}

		sort.Slice(summary.CorruptedBlocks, func(i, j int) bool {
			return summary.CorruptedBlocks[i].BlockID < summary.CorruptedBlocks[j].BlockID
		})
	}

	return summary
}

type blockEntry struct {
	blockID      string
	hasBlockFile bool
}

func (s *Server) listBlockEntries() ([]blockEntry, error) {
	entries, err := os.ReadDir(s.storageDir)
	if err != nil {
		return nil, fmt.Errorf("list blocks: %w", err)
	}

	ids := make(map[string]*blockEntry)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		hasBlockFile := true

		if strings.HasSuffix(name, checksumSuffix) {
			name = strings.TrimSuffix(name, checksumSuffix)
			hasBlockFile = false
		}

		blockID, err := decodeBlockID(name)
		if err != nil {
			log.Printf("dataserver: skip malformed block filename %s: %v", entry.Name(), err)
			continue
		}

		info, ok := ids[blockID]
		if !ok {
			info = &blockEntry{blockID: blockID}
			ids[blockID] = info
		}

		if hasBlockFile {
			info.hasBlockFile = true
		}
	}

	result := make([]blockEntry, 0, len(ids))
	for _, entry := range ids {
		result = append(result, *entry)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].blockID < result[j].blockID
	})

	return result, nil
}

func decodeBlockID(safeName string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(safeName)
	if err != nil {
		return "", fmt.Errorf("decode block name %s: %w", safeName, err)
	}

	return string(data), nil
}

const (
	checksumSuffix      = ".crc"
	errChecksumMismatch = "checksum mismatch"
)

func (s *Server) checksumPath(blockID string) string {
	return s.blockPath(blockID) + checksumSuffix
}

func (s *Server) blockPath(blockID string) string {
	safe := base64.RawURLEncoding.EncodeToString([]byte(blockID))
	return filepath.Join(s.storageDir, safe)
}

func (s *Server) verifyBlockIntegrity(blockID string) protocol.BlockVerification {
	result := protocol.BlockVerification{BlockID: blockID, LastChecked: time.Now().UTC()}
	path := s.blockPath(blockID)
	checksumPath := s.checksumPath(blockID)

	s.mu.RLock()
	data, err := os.ReadFile(path)
	s.mu.RUnlock()

	if err != nil {
		if os.IsNotExist(err) {
			result.Error = "block not found"
		} else {
			result.Error = fmt.Sprintf("read block: %v", err)
		}

		return result
	}

	s.mu.RLock()
	storedChecksum, err := readChecksum(checksumPath)
	s.mu.RUnlock()

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			result.Error = "checksum file missing"
		} else {
			result.Error = fmt.Sprintf("read checksum: %v", err)
		}

		return result
	}

	if crc32.ChecksumIEEE(data) != storedChecksum {
		result.Error = errChecksumMismatch
		return result
	}

	result.Healthy = true
	result.Error = ""

	return result
}
