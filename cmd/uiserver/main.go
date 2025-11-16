package main

import (
	"flag"
	"io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"filefly/ui"
)

func main() {
	addr := flag.String("addr", ":8090", "address to expose the UI and proxy")
	metadataAPI := flag.String("metadata-api", "http://localhost:8090", "base URL for the metadata HTTP API (proxied at /api)")
	flag.Parse()

	target, err := url.Parse(*metadataAPI)
	if err != nil {
		log.Fatalf("invalid --metadata-api value: %v", err)
	}

	staticFS, err := fs.Sub(ui.Static, "static")
	if err != nil {
		log.Fatalf("failed to load UI assets: %v", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	mux := http.NewServeMux()
	mux.Handle("/api/", proxy)
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	log.Printf("UI server listening on %s (proxying API to %s)", *addr, target)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatalf("ui server failed: %v", err)
	}
}
