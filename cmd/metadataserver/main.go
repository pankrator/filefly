package main

import (
	"flag"
	"log"
	"strings"

	"filefly/internal/metadataserver"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	blockSize := flag.Int("block-size", 1024, "block size in bytes")
	dataServers := flag.String("data-servers", ":8081", "comma separated list of data server addresses")
	flag.Parse()

	var servers []string
	for _, s := range strings.Split(*dataServers, ",") {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			servers = append(servers, trimmed)
		}
	}

	srv := metadataserver.New(*addr, *blockSize, servers)
	if err := srv.Listen(); err != nil {
		log.Fatalf("metadata server failed: %v", err)
	}
}
