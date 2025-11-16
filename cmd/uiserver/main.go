package main

import (
	"flag"
	"io/fs"
	"log"
	"net/http"

	"filefly/ui"
)

func main() {
	addr := flag.String("addr", ":8090", "address to expose the UI and REST API")
	metadataAddr := flag.String("metadata-server", ":8080", "TCP address of the metadata server")
	flag.Parse()

	staticFS, err := fs.Sub(ui.Static, "static")
	if err != nil {
		log.Fatalf("failed to load UI assets: %v", err)
	}

	srv := newUIServer(*metadataAddr, http.FS(staticFS))

	log.Printf("UI server listening on %s (metadata=%s)", *addr, *metadataAddr)
	if err := http.ListenAndServe(*addr, srv.routes()); err != nil {
		log.Fatalf("ui server failed: %v", err)
	}
}
