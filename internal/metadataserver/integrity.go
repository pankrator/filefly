package metadataserver

import (
	"log"
	"sync"
	"time"
)

type dataIntegrityAuditor struct {
	server   *Server
	interval time.Duration

	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func newDataIntegrityAuditor(server *Server, interval time.Duration) *dataIntegrityAuditor {
	if server == nil || interval <= 0 {
		return nil
	}

	return &dataIntegrityAuditor{
		server:   server,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

func (a *dataIntegrityAuditor) Start() {
	if a == nil {
		return
	}

	a.wg.Add(1)
	go a.run()
}

func (a *dataIntegrityAuditor) Stop() {
	if a == nil {
		return
	}

	a.once.Do(func() { close(a.stop) })
	a.wg.Wait()
}

func (a *dataIntegrityAuditor) run() {
	defer a.wg.Done()

	a.scanAll()

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.scanAll()
		case <-a.stop:
			return
		}
	}
}

func (a *dataIntegrityAuditor) scanAll() {
	if a == nil || a.server == nil || a.server.dataClient == nil {
		return
	}

	for _, addr := range a.server.dataServersSnapshot() {
		if addr == "" {
			continue
		}

		a.verifyAndRepair(addr)
	}
}

func (a *dataIntegrityAuditor) verifyAndRepair(addr string) {
	if a == nil || a.server == nil || a.server.dataClient == nil {
		return
	}

	summary, err := a.server.dataClient.VerifyAll(addr)
	if err != nil {
		a.server.recordVerification(addr, nil, err)
		log.Printf("metadata: verify %s failed: %v", addr, err)

		return
	}

	a.server.recordVerification(addr, summary, nil)

	if summary == nil || summary.UnhealthyBlocks == 0 || len(summary.CorruptedBlocks) == 0 {
		return
	}

	for _, block := range summary.CorruptedBlocks {
		if block.BlockID == "" {
			continue
		}

		if err := a.server.repairReplica(addr, block.BlockID); err != nil {
			log.Printf("metadata: repair %s on %s failed: %v", block.BlockID, addr, err)
			continue
		}

		log.Printf("metadata: repaired block %s on %s", block.BlockID, addr)
	}
}
