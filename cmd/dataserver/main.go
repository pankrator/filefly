package main

import (
	"flag"
	"log"
	"time"

	"filefly/internal/dataserver"
)

func main() {
	addr := flag.String("addr", ":8081", "address to listen on")
	storageDir := flag.String("storage_dir", "./blocks", "directory used to persist blocks")
	metadataServer := flag.String("metadata-server", "", "address of the metadata (name) server to register with")
	metadataPing := flag.Duration("metadata-ping-interval", 15*time.Second, "how often to ping the metadata server for registration")
	flag.Parse()

	srv, err := dataserver.New(*addr, *storageDir)
	if err != nil {
		log.Fatalf("data server init failed: %v", err)
	}
	if *metadataServer != "" {
		srv.EnableMetadataRegistration(*metadataServer, *metadataPing)
	}
	if err := srv.Listen(); err != nil {
		log.Fatalf("data server failed: %v", err)
	}
}
