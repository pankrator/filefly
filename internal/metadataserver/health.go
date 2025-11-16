package metadataserver

import (
	"sync"
	"time"

	"filefly/internal/protocol"
)

const defaultHealthCheckInterval = 10 * time.Second

// dataHealthMonitor periodically pings data servers and keeps track of their health.
type dataHealthMonitor struct {
	client   *dataServerClient
	servers  []string
	interval time.Duration

	mu       sync.RWMutex
	statuses map[string]protocol.DataServerHealth

	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

func newDataHealthMonitor(servers []string, client *dataServerClient, interval time.Duration) *dataHealthMonitor {
	if interval <= 0 {
		interval = defaultHealthCheckInterval
	}
	return &dataHealthMonitor{
		client:   client,
		servers:  append([]string(nil), servers...),
		interval: interval,
		statuses: make(map[string]protocol.DataServerHealth),
		stop:     make(chan struct{}),
	}
}

func (m *dataHealthMonitor) Start() {
	if m == nil {
		return
	}
	m.wg.Add(1)
	go m.run()
}

func (m *dataHealthMonitor) Stop() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		close(m.stop)
	})
	m.wg.Wait()
}

func (m *dataHealthMonitor) run() {
	defer m.wg.Done()
	m.checkAll()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.checkAll()
		case <-m.stop:
			return
		}
	}
}

func (m *dataHealthMonitor) checkAll() {
	for _, addr := range m.snapshotServers() {
		m.check(addr)
	}
}

func (m *dataHealthMonitor) check(addr string) {
	if m.client == nil {
		m.record(addr, time.Now().UTC(), false, "metadata server has no data client")
		return
	}
	err := m.client.Ping(addr)
	now := time.Now().UTC()
	if err != nil {
		m.record(addr, now, false, err.Error())
		return
	}
	m.record(addr, now, true, "")
}

func (m *dataHealthMonitor) record(addr string, ts time.Time, healthy bool, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := m.statuses[addr]
	status.Address = addr
	status.LastChecked = ts
	status.Healthy = healthy
	if healthy {
		status.LastPong = ts
		status.Error = ""
	} else {
		status.Error = errMsg
	}
	m.statuses[addr] = status
}

func (m *dataHealthMonitor) List() []protocol.DataServerHealth {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]protocol.DataServerHealth, 0, len(m.servers))
	for _, addr := range m.servers {
		if status, ok := m.statuses[addr]; ok {
			result = append(result, status)
			continue
		}
		result = append(result, protocol.DataServerHealth{Address: addr})
	}
	return result
}

func (m *dataHealthMonitor) EnsureServer(addr string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	for _, existing := range m.servers {
		if existing == addr {
			m.mu.Unlock()
			return
		}
	}
	m.servers = append(m.servers, addr)
	m.mu.Unlock()
}

func (m *dataHealthMonitor) CheckNow(addr string) {
	if m == nil || addr == "" {
		return
	}
	go m.check(addr)
}

func (m *dataHealthMonitor) snapshotServers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string(nil), m.servers...)
}
