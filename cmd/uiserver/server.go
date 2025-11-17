package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
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
	mux.HandleFunc("/api/data-servers/verify", s.handleVerifyDataServer)
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

func (s *uiServer) handleVerifyDataServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close() //nolint:errcheck

	var payload struct {
		Address string `json:"address"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	addr := strings.TrimSpace(payload.Address)
	if addr == "" {
		http.Error(w, "missing address", http.StatusBadRequest)
		return
	}

	server, err := s.metadata.verifyDataServer(addr)
	if err != nil {
		http.Error(w, fmt.Sprintf("verify %s: %v", addr, err), http.StatusBadGateway)
		return
	}

	respondJSON(w, map[string]any{"server": server})
}

func (s *uiServer) handleListFiles(w http.ResponseWriter, _ *http.Request) {
	files, err := s.metadata.listFiles()
	if err != nil {
		http.Error(w, fmt.Sprintf("list files: %v", err), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]any{"files": files})
}

//nolint:funlen
func (s *uiServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll() //nolint:errcheck
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file", http.StatusBadRequest)
		return
	}
	defer file.Close() //nolint:errcheck

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" && header != nil {
		name = header.Filename
	}

	if name == "" {
		http.Error(w, "missing file name", http.StatusBadRequest)
		return
	}

	totalSize, err := uploadFileSize(file, header)
	if err != nil {
		http.Error(w, fmt.Sprintf("determine file size: %v", err), http.StatusBadRequest)
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

	log.Printf("ui: upload request for %s (%d bytes, replicas=%d)", name, totalSize, replicas)

	meta, err := s.metadata.planFile(name, totalSize, replicas)
	if err != nil {
		http.Error(w, fmt.Sprintf("plan file: %v", err), http.StatusInternalServerError)
		return
	}

	if err := rewindMultipartFile(file); err != nil {
		http.Error(w, fmt.Sprintf("rewind upload data: %v", err), http.StatusInternalServerError)
		return
	}

	if err := s.transfer.UploadBlocksFrom(meta.Blocks, file); err != nil {
		http.Error(w, fmt.Sprintf("upload blocks: %v", err), http.StatusInternalServerError)
		return
	}

	if err := s.metadata.completeFile(meta); err != nil {
		http.Error(w, fmt.Sprintf("finalize metadata: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("ui: upload completed for %s (%d blocks)", meta.Name, len(meta.Blocks))
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
		log.Printf("ui: delete requested for %s", name)

		if err := s.metadata.deleteFile(name); err != nil {
			s.handleMetadataError(w, r, err)
			return
		}

		log.Printf("ui: deleted %s", name)
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

	log.Printf("ui: downloading %s (%d blocks)", meta.Name, len(meta.Blocks))

	data, err := s.transfer.DownloadFile(meta, s.downloadConcurrency)
	if err != nil {
		http.Error(w, fmt.Sprintf("download file: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("ui: download complete for %s (%d bytes)", meta.Name, len(data))

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

func uploadFileSize(file multipart.File, header *multipart.FileHeader) (int, error) {
	if header != nil && header.Size >= 0 {
		if header.Size > int64(math.MaxInt) {
			return 0, fmt.Errorf("file too large: %d bytes exceeds supported size", header.Size)
		}

		if err := rewindMultipartFile(file); err != nil {
			return 0, err
		}

		return int(header.Size), nil
	}

	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	if size > int64(math.MaxInt) {
		return 0, fmt.Errorf("file too large: %d bytes exceeds supported size", size)
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	return int(size), nil
}

func rewindMultipartFile(file multipart.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return nil
}

func respondJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
