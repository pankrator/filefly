package main

import (
	"flag"
	"log"

	"filefly/internal/dataserver"
)

func main() {
	addr := flag.String("addr", ":8081", "address to listen on")
	flag.Parse()

	srv := dataserver.New(*addr)
	if err := srv.Listen(); err != nil {
		log.Fatalf("data server failed: %v", err)
	}
}
