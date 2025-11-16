package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"filefly/internal/transfer"
)

func newUIServer(metadataAddr string, static http.FileSystem, downloadConcurrency int) *uiServer {
	if downloadConcurrency <= 0 {
		downloadConcurrency = 1
	}
	return &uiServer{
		metadata:            newMetadataClient(metadataAddr),
		static:              static,
		downloadConcurrency: downloadConcurrency,
		transfer:            transfer.NewClient(),
	}
}

type uiServer struct {
	metadata            *metadataClient
	static              http.FileSystem
	downloadConcurrency int
	transfer            *transfer.Client
}

func (s *uiServer) Close() {
	if s.transfer != nil {
		s.transfer.Close()
	}
}

func (s *uiServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/files", s.handleFiles)
	mux.HandleFunc("/api/files/", s.handleFileDetail)
	mux.HandleFunc("/api/data-servers", s.handleDataServers)
	mux.Handle("/", http.FileServer(s.static))
	return mux
}

func (s *uiServer) handleFiles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleListFiles(w, r)
	case http.MethodPost:
		s.handleUpload(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *uiServer) handleDataServers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	servers, err := s.metadata.listDataServers()
	if err != nil {
		http.Error(w, fmt.Sprintf("list data servers: %v", err), http.StatusInternalServerError)
		return
	}
	respondJSON(w, map[string]any{"servers": servers})
}

func (s *uiServer) handleListFiles(w http.ResponseWriter, _ *http.Request) {
	files, err := s.metadata.listFiles()
	if err != nil {
		http.Error(w, fmt.Sprintf("list files: %v", err), http.StatusInternalServerError)
		return
	}
	respondJSON(w, map[string]any{"files": files})
}

func (s *uiServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "failed to read file", http.StatusInternalServerError)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" && header != nil {
		name = header.Filename
	}
	if name == "" {
		http.Error(w, "missing file name", http.StatusBadRequest)
		return
	}

	replicas := 0
	if raw := strings.TrimSpace(r.FormValue("replicas")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			http.Error(w, "replicas must be a positive number", http.StatusBadRequest)
			return
		}
		replicas = value
	}

	meta, err := s.metadata.planFile(name, len(data), replicas)
	if err != nil {
		http.Error(w, fmt.Sprintf("plan file: %v", err), http.StatusInternalServerError)
		return
	}

	if err := s.transfer.UploadBlocks(meta.Blocks, data); err != nil {
		http.Error(w, fmt.Sprintf("upload blocks: %v", err), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]any{"file": meta})
}

func (s *uiServer) handleFileDetail(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/files/")
	if trimmed == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(trimmed, "/")
	name, err := url.PathUnescape(parts[0])
	if err != nil {
		http.Error(w, "invalid file name", http.StatusBadRequest)
		return
	}
	if len(parts) > 1 {
		if parts[1] == "content" {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.handleDownload(w, r, name)
			return
		}
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		meta, err := s.metadata.getMetadata(name)
		if err != nil {
			s.handleMetadataError(w, r, err)
			return
		}
		respondJSON(w, map[string]any{"file": meta})
	case http.MethodDelete:
		if err := s.metadata.deleteFile(name); err != nil {
			s.handleMetadataError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *uiServer) handleDownload(w http.ResponseWriter, r *http.Request, name string) {
	meta, err := s.metadata.getMetadata(name)
	if err != nil {
		s.handleMetadataError(w, r, err)
		return
	}

	data, err := s.transfer.DownloadFile(meta, s.downloadConcurrency)
	if err != nil {
		http.Error(w, fmt.Sprintf("download file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", path.Base(meta.Name)))
	_, _ = w.Write(data)
}

func (s *uiServer) handleMetadataError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errNotFound) {
		http.NotFound(w, r)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func respondJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
