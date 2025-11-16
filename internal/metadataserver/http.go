package metadataserver

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
)

//go:embed web/*
var webFS embed.FS

// ListenHTTP starts the HTTP server that hosts the UI and API.
func (s *Server) ListenHTTP(addr string) error {
	handler, err := s.httpHandler()
	if err != nil {
		return err
	}
	log.Printf("metadata HTTP UI listening on %s", addr)
	return http.ListenAndServe(addr, handler)
}

func (s *Server) httpHandler() (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/files", s.handleFiles)
	mux.HandleFunc("/api/files/", s.handleFileDetail)

	staticFS, err := fs.Sub(webFS, "web")
	if err != nil {
		return nil, fmt.Errorf("prepare web assets: %w", err)
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.Handle("/", fileServer)
	return mux, nil
}

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleListFiles(w, r)
	case http.MethodPost:
		s.handleUploadFile(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleListFiles(w http.ResponseWriter, _ *http.Request) {
	files := s.ListFiles()
	respondJSON(w, map[string]any{"files": files})
}

func (s *Server) handleUploadFile(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
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

	meta, err := s.StoreFileBytes(name, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]any{"file": meta})
}

func (s *Server) handleFileDetail(w http.ResponseWriter, r *http.Request) {
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
			s.handleDownloadFile(w, name)
			return
		}
		http.NotFound(w, r)
		return
	}

	s.mu.RLock()
	meta, ok := s.files[name]
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	respondJSON(w, map[string]any{"file": meta})
}

func (s *Server) handleDownloadFile(w http.ResponseWriter, name string) {
	data, meta, err := s.FetchFileBytes(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	if meta != nil {
		safeName := path.Base(meta.Name)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", safeName))
	}
	_, _ = w.Write(data)
}

func respondJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
