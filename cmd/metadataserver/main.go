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
	dataServers := flag.String("data-servers", ":8081", "comma separated list of internal data server addresses")
	advertisedDataServers := flag.String(
		"advertised-data-servers",
		"",
		"comma separated list of advertised data server addresses matching data-servers",
	)
	metadataFile := flag.String("metadata-file", "metadata.json", "path to persist metadata snapshots")
	persistInterval := flag.Duration("persist-interval", 30*time.Second, "how often to persist metadata")
	integrityInterval := flag.Duration(
		"integrity-interval",
		5*time.Minute,
		"how often to verify data servers (<=0 disables)",
	)

	flag.Parse()

	var privateServers []string

	for _, s := range strings.Split(*dataServers, ",") {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			privateServers = append(privateServers, trimmed)
		}
	}

	var advertisedServers []string

	for _, s := range strings.Split(*advertisedDataServers, ",") {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			advertisedServers = append(advertisedServers, trimmed)
		}
	}

	srv, err := metadataserver.New(
		*addr,
		*blockSize,
		privateServers,
		advertisedServers,
		*metadataFile,
		*persistInterval,
		*integrityInterval,
	)
	if err != nil {
		log.Fatalf("metadata server init failed: %v", err)
	}

	if err := srv.Listen(); err != nil {
		log.Fatalf("metadata server failed: %v", err)
	}
}
