package metadataserver

import (
	"errors"
	"sort"
	"sync"

	"filefly/internal/protocol"
)

var (
	ErrFileNotFound      = errors.New("file not found")
	ErrDeleteInProgress  = errors.New("delete already in progress")
	ErrDeleteNotReserved = errors.New("delete not reserved")
)

type metadataStore interface {
	Save(meta protocol.FileMetadata) error
	Get(name string) (protocol.FileMetadata, bool)
	List() []protocol.FileMetadata
	BeginDelete(name string) (protocol.FileMetadata, error)
	CompleteDelete(name string)
	CancelDelete(name string)
	Snapshot() map[string]protocol.FileMetadata
	Restore(files map[string]protocol.FileMetadata)
}

func newMetadataStore() metadataStore {
	return &memoryStore{
		files:    make(map[string]protocol.FileMetadata),
		deleting: make(map[string]struct{}),
	}
}

type memoryStore struct {
	mu       sync.RWMutex
	files    map[string]protocol.FileMetadata
	deleting map[string]struct{}
}

func (s *memoryStore) Save(meta protocol.FileMetadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, deleting := s.deleting[meta.Name]; deleting {
		return ErrDeleteInProgress
	}

	s.files[meta.Name] = meta

	return nil
}

func (s *memoryStore) Get(name string) (protocol.FileMetadata, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.files[name]

	return meta, ok
}

func (s *memoryStore) List() []protocol.FileMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()

	files := make([]protocol.FileMetadata, 0, len(s.files))
	for _, meta := range s.files {
		files = append(files, meta)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })

	return files
}

func (s *memoryStore) BeginDelete(name string) (protocol.FileMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, ok := s.files[name]
	if !ok {
		return protocol.FileMetadata{}, ErrFileNotFound
	}

	if _, deleting := s.deleting[name]; deleting {
		return protocol.FileMetadata{}, ErrDeleteInProgress
	}

	s.deleting[name] = struct{}{}

	return meta, nil
}

func (s *memoryStore) CompleteDelete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.files, name)
	delete(s.deleting, name)
}

func (s *memoryStore) CancelDelete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.deleting, name)
}

func (s *memoryStore) Snapshot() map[string]protocol.FileMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[string]protocol.FileMetadata, len(s.files))
	for name, meta := range s.files {
		snapshot[name] = meta
	}

	return snapshot
}

func (s *memoryStore) Restore(files map[string]protocol.FileMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if files == nil {
		files = make(map[string]protocol.FileMetadata)
	}

	s.files = files
	s.deleting = make(map[string]struct{})
}
