package dataserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"filefly/internal/protocol"
)

const (
	defaultMetadataPingInterval = 15 * time.Second
	metadataDialTimeout         = 5 * time.Second
)

type metadataRegistrar struct {
	metadataAddr string
	dataAddr     string
	interval     time.Duration

	mu          sync.Mutex
	lastSuccess bool

	stop chan struct{}
	wg   sync.WaitGroup
}

func newMetadataRegistrar(metadataAddr, dataAddr string, interval time.Duration) *metadataRegistrar {
	if interval <= 0 {
		interval = defaultMetadataPingInterval
	}
	return &metadataRegistrar{
		metadataAddr: metadataAddr,
		dataAddr:     dataAddr,
		interval:     interval,
		stop:         make(chan struct{}),
	}
}

// EnableMetadataRegistration configures the data server to periodically ping the
// metadata server so it can re-register after interruptions.
func (s *Server) EnableMetadataRegistration(metadataAddr string, interval time.Duration) {
	if s == nil || metadataAddr == "" {
		return
	}
	s.metadataAddr = metadataAddr
	s.metadataInterval = interval
}

func (r *metadataRegistrar) Start() {
	if r == nil {
		return
	}
	r.wg.Add(1)
	go r.run()
}

func (r *metadataRegistrar) Stop() {
	if r == nil {
		return
	}
	close(r.stop)
	r.wg.Wait()
}

func (r *metadataRegistrar) run() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	r.sendHeartbeat()
	for {
		select {
		case <-ticker.C:
			r.sendHeartbeat()
		case <-r.stop:
			return
		}
	}
}

func (r *metadataRegistrar) sendHeartbeat() {
	conn, err := net.DialTimeout("tcp", r.metadataAddr, metadataDialTimeout)
	if err != nil {
		r.reportFailure(err)
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(metadataDialTimeout))

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))
	req := protocol.MetadataRequest{Command: "register_data_server", DataServerAddr: r.dataAddr}
	if err := enc.Encode(req); err != nil {
		r.reportFailure(err)
		return
	}
	var resp protocol.MetadataResponse
	if err := dec.Decode(&resp); err != nil {
		r.reportFailure(err)
		return
	}
	if resp.Status != "ok" {
		if resp.Error == "" {
			resp.Error = "metadata server rejected registration"
		}
		r.reportFailure(fmt.Errorf(resp.Error))
		return
	}
	r.reportSuccess()
}

func (r *metadataRegistrar) reportSuccess() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.lastSuccess {
		log.Printf("dataserver: registered with metadata server %s", r.metadataAddr)
	}
	r.lastSuccess = true
}

func (r *metadataRegistrar) reportFailure(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSuccess {
		log.Printf("dataserver: lost metadata server %s connection: %v", r.metadataAddr, err)
	} else {
		log.Printf("dataserver: metadata server %s unavailable: %v", r.metadataAddr, err)
	}
	r.lastSuccess = false
}
