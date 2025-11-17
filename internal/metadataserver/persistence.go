package metadataserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"filefly/internal/protocol"
)

type diskSnapshot struct {
	Files map[string]protocol.FileMetadata `json:"files"`
}

type diskPersister struct {
	path string
}

func newDiskPersister(path string) *diskPersister {
	return &diskPersister{path: path}
}

func (p *diskPersister) Load(store metadataStore) error {
	if p == nil || p.path == "" {
		return nil
	}

	f, err := os.Open(p.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("open metadata file: %w", err)
	}

	defer f.Close() //nolint:errcheck

	var snapshot diskSnapshot

	dec := json.NewDecoder(f)
	if err := dec.Decode(&snapshot); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}

		return fmt.Errorf("decode metadata file: %w", err)
	}

	store.Restore(snapshot.Files)
	log.Printf("metadata server loaded %d entries from %s", len(snapshot.Files), p.path)

	return nil
}

func (p *diskPersister) Persist(store metadataStore) error {
	if p == nil || p.path == "" {
		return nil
	}

	snapshot := store.Snapshot()

	bytes, err := json.MarshalIndent(diskSnapshot{Files: snapshot}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata snapshot: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(p.path), 0o755); err != nil {
		return fmt.Errorf("ensure metadata directory: %w", err)
	}

	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, bytes, 0o600); err != nil {
		return fmt.Errorf("write temp metadata file: %w", err)
	}

	if err := os.Rename(tmp, p.path); err != nil {
		return fmt.Errorf("replace metadata file: %w", err)
	}

	return nil
}

func (p *diskPersister) Start(store metadataStore, freq time.Duration) {
	if p == nil || p.path == "" || freq <= 0 {
		return
	}

	if err := p.Persist(store); err != nil {
		log.Printf("metadata persistence error: %v", err)
	}

	go func() {
		ticker := time.NewTicker(freq)
		defer ticker.Stop()

		for range ticker.C {
			if err := p.Persist(store); err != nil {
				log.Printf("metadata persistence error: %v", err)
			}
		}
	}()
}
