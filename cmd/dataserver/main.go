package main

import (
	"flag"
	"log"

	"filefly/internal/dataserver"
)

func main() {
	addr := flag.String("addr", ":8081", "address to listen on")
	storageDir := flag.String("storage_dir", "./blocks", "directory used to persist blocks")
	flag.Parse()

	srv, err := dataserver.New(*addr, *storageDir)
	if err != nil {
		log.Fatalf("data server init failed: %v", err)
	}
	if err := srv.Listen(); err != nil {
		log.Fatalf("data server failed: %v", err)
	}
}
