package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"filefly/internal/metadataserver"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP address for the internal metadata protocol")
	blockSize := flag.Int("block-size", 1024, "block size in bytes")
	dataServers := flag.String("data-servers", ":8081", "comma separated list of data server addresses")
	metadataFile := flag.String("metadata-file", "metadata.json", "path to persist metadata snapshots")
	persistInterval := flag.Duration("persist-interval", 30*time.Second, "how often to persist metadata")
	flag.Parse()

	var servers []string
	for _, s := range strings.Split(*dataServers, ",") {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			servers = append(servers, trimmed)
		}
	}

	srv := metadataserver.New(*addr, *blockSize, servers, *metadataFile, *persistInterval)

	if err := srv.Listen(); err != nil {
		log.Fatalf("metadata server failed: %v", err)
	}
}
