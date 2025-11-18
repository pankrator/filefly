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
	verifyInterval := flag.Duration(
		"verify-interval",
		5*time.Minute,
		"how often to scan stored blocks for corruption (0 to disable)",
	)

	flag.Parse()

	srv, err := dataserver.New(*addr, *storageDir, dataserver.WithVerificationInterval(*verifyInterval))
	if err != nil {
		log.Fatalf("data server init failed: %v", err)
	}

	if err := srv.Listen(); err != nil {
		log.Fatalf("data server failed: %v", err)
	}
}
