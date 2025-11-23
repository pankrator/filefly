package main

import (
	"flag"
	"log"
	"strings"
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
	repairSources := flag.String(
		"allowed-repair-sources",
		"",
		"comma separated list of trusted data servers that can serve repairs",
	)

	flag.Parse()

	var allowedSources []string

	for _, src := range strings.Split(*repairSources, ",") {
		trimmed := strings.TrimSpace(src)
		if trimmed != "" {
			allowedSources = append(allowedSources, trimmed)
		}
	}

	srv, err := dataserver.New(
		*addr,
		*storageDir,
		dataserver.WithVerificationInterval(*verifyInterval),
		dataserver.WithAllowedRepairSources(allowedSources),
	)
	if err != nil {
		log.Fatalf("data server init failed: %v", err)
	}

	if err := srv.Listen(); err != nil {
		log.Fatalf("data server failed: %v", err)
	}
}
