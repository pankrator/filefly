package transfer

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"filefly/internal/protocol"
)

const replicaFetchTimeout = 5 * time.Second

// Client coordinates the upload and download of file blocks across data servers.
// It hides connection pooling concerns from callers and focuses on the transfer
// use cases so consumers can concentrate on higher-level workflows.
type Client struct {
	pool        *connCache
	dialTimeout time.Duration
}

func (c *Client) applyReadDeadline(conn *cachedConn) {
	if c == nil || conn == nil || conn.conn == nil || c.dialTimeout <= 0 {
		return
	}
	_ = conn.conn.SetReadDeadline(time.Now().Add(c.dialTimeout))
}

func (c *Client) applyWriteDeadline(conn *cachedConn) {
	if c == nil || conn == nil || conn.conn == nil || c.dialTimeout <= 0 {
		return
	}
	_ = conn.conn.SetWriteDeadline(time.Now().Add(c.dialTimeout))
}

type clientConfig struct {
	dialTimeout        time.Duration
	maxDataServerConns int
}

// ClientOption configures a transfer client during creation.
type ClientOption func(*clientConfig)

// WithDialTimeout overrides the default timeout used when communicating with
// data servers.
func WithDialTimeout(d time.Duration) ClientOption {
	return func(cfg *clientConfig) {
		cfg.dialTimeout = d
	}
}

// WithMaxDataServerConnections configures how many pooled connections can be
// opened to a single data server. Values less than or equal to zero will allow
// unlimited connections.
func WithMaxDataServerConnections(n int) ClientOption {
	return func(cfg *clientConfig) {
		cfg.maxDataServerConns = n
	}
}

// NewClient creates a transfer client with a pre-warmed connection pool.
func NewClient(opts ...ClientOption) *Client {
	cfg := clientConfig{
		dialTimeout:        replicaFetchTimeout,
		maxDataServerConns: 1,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &Client{
		pool:        newConnCache(cfg.maxDataServerConns, cfg.dialTimeout),
		dialTimeout: cfg.dialTimeout,
	}
}

// Close releases any cached connections owned by the client.
func (c *Client) Close() {
	if c == nil || c.pool == nil {
		return
	}
	c.pool.CloseAll()
}

// UploadBlocks streams the provided data to the configured data servers based
// on the block placement metadata.
func (c *Client) UploadBlocks(blocks []protocol.BlockRef, data []byte) error {
	if c == nil {
		return fmt.Errorf("transfer client is nil")
	}
	offset := 0
	for _, block := range blocks {
		replicas := normalizeBlockReplicas(block)
		if len(replicas) == 0 {
			return fmt.Errorf("block %s has no replicas to upload", block.ID)
		}
		end := offset + block.Size
		if end > len(data) {
			return fmt.Errorf("block %s exceeds file size", block.ID)
		}
		chunk := data[offset:end]
		if err := c.uploadBlock(block.ID, replicas, chunk); err != nil {
			return err
		}
		offset = end
	}
	if offset != len(data) {
		return fmt.Errorf("plan left %d bytes unused", len(data)-offset)
	}
	return nil
}

func (c *Client) uploadBlock(blockID string, replicas []protocol.BlockReplica, data []byte) error {
	var successes int
	var lastErr error
	for _, replica := range replicas {
		if err := c.uploadReplica(blockID, replica, data); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			log.Printf("transfer: failed to store block %s on %s: %v", blockID, replica.DataServer, err)
			continue
		}
		successes++
	}
	if successes != len(replicas) {
		if lastErr == nil {
			lastErr = fmt.Errorf("replica count mismatch: stored %d/%d copies", successes, len(replicas))
		}
		return fmt.Errorf("upload block %s: %w", blockID, lastErr)
	}
	return nil
}

func (c *Client) uploadReplica(blockID string, replica protocol.BlockReplica, data []byte) error {
	conn, release, err := c.pool.Acquire(replica.DataServer)
	if err != nil {
		return fmt.Errorf("connect to data server %s: %w", replica.DataServer, err)
	}
	drop := false
	defer func() { release(drop) }()
	conn.resetDeadlines()
	req := protocol.DataServerRequest{
		Command: "store",
		BlockID: blockID,
		Data:    base64.StdEncoding.EncodeToString(data),
	}
	c.applyWriteDeadline(conn)
	if err := conn.enc.Encode(req); err != nil {
		drop = true
		return fmt.Errorf("send store to %s: %w", replica.DataServer, err)
	}
	var resp protocol.DataServerResponse
	c.applyReadDeadline(conn)
	if err := conn.dec.Decode(&resp); err != nil {
		drop = true
		return fmt.Errorf("decode response from %s: %w", replica.DataServer, err)
	}
	if resp.Status != "ok" {
		if resp.Error == "" {
			resp.Error = "data server returned error"
		}
		return fmt.Errorf("data server %s: %s", replica.DataServer, resp.Error)
	}
	return nil
}

// DownloadFile retrieves the full content of a file according to the provided
// metadata. Blocks are fetched concurrently up to maxConcurrency.
func (c *Client) DownloadFile(meta *protocol.FileMetadata, maxConcurrency int) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata is nil")
	}
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	blks := meta.Blocks
	chunks := make([][]byte, len(blks))
	sem := make(chan struct{}, maxConcurrency)
	var eg errgroup.Group
	for i := range blks {
		i := i
		block := blks[i]
		eg.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()
			chunk, err := c.fetchBlockWithFailover(block)
			if err != nil {
				return err
			}
			chunks[i] = chunk
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, meta.TotalSize)
	for _, chunk := range chunks {
		buf = append(buf, chunk...)
	}
	return buf, nil
}

func (c *Client) fetchBlockWithFailover(block protocol.BlockRef) ([]byte, error) {
	replicas := normalizeBlockReplicas(block)
	if len(replicas) == 0 {
		return nil, fmt.Errorf("block %s has no replicas", block.ID)
	}
	var lastErr error
	for _, replica := range replicas {
		chunk, err := c.pullBlock(replica.DataServer, block.ID)
		if err == nil {
			return chunk, nil
		}
		lastErr = err
		log.Printf("transfer: failed to download block %s from %s: %v", block.ID, replica.DataServer, err)
	}
	return nil, fmt.Errorf("retrieve block %s: %w", block.ID, lastErr)
}

func (c *Client) pullBlock(addr, blockID string) ([]byte, error) {
	conn, release, err := c.pool.Acquire(addr)
	if err != nil {
		return nil, fmt.Errorf("connect to data server %s: %w", addr, err)
	}
	drop := false
	defer func() { release(drop) }()
	req := protocol.DataServerRequest{
		Command: "retrieve",
		BlockID: blockID,
	}
	c.applyWriteDeadline(conn)
	if err := conn.enc.Encode(req); err != nil {
		drop = true
		return nil, fmt.Errorf("send retrieve to %s: %w", addr, err)
	}
	var resp protocol.DataServerResponse
	c.applyReadDeadline(conn)
	if err := conn.dec.Decode(&resp); err != nil {
		drop = true
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	if resp.Status != "ok" {
		return nil, fmt.Errorf("data server %s error: %s", addr, resp.Error)
	}
	data, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 from %s: %w", addr, err)
	}
	return data, nil
}

func normalizeBlockReplicas(block protocol.BlockRef) []protocol.BlockReplica {
	if len(block.Replicas) > 0 {
		return block.Replicas
	}
	if block.DataServer != "" {
		return []protocol.BlockReplica{{DataServer: block.DataServer}}
	}
	return nil
}

type cachedConn struct {
	enc   *json.Encoder
	dec   *json.Decoder
	conn  net.Conn
	alive bool
	inUse bool
}

func (c *cachedConn) resetDeadlines() {
	if c == nil || c.conn == nil {
		return
	}
	var zero time.Time
	_ = c.conn.SetDeadline(zero)
	_ = c.conn.SetReadDeadline(zero)
	_ = c.conn.SetWriteDeadline(zero)
}

func (c *cachedConn) ping() error {
	if c == nil {
		return fmt.Errorf("cached connection is nil")
	}
	if err := c.enc.Encode(protocol.DataServerRequest{Command: "ping"}); err != nil {
		return err
	}
	var resp protocol.DataServerResponse
	if err := c.dec.Decode(&resp); err != nil {
		return err
	}
	if resp.Status != "ok" || !resp.Pong {
		if resp.Error == "" {
			resp.Error = "data server ping failed"
		}
		return fmt.Errorf(resp.Error)
	}
	return nil
}

type connCache struct {
	mu          sync.Mutex
	cond        *sync.Cond
	conns       map[string][]*cachedConn
	maxPerAddr  int
	dialTimeout time.Duration
}

func newConnCache(maxPerAddr int, dialTimeout time.Duration) *connCache {
	c := &connCache{
		conns:       make(map[string][]*cachedConn),
		maxPerAddr:  maxPerAddr,
		dialTimeout: dialTimeout,
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *connCache) Acquire(addr string) (*cachedConn, func(drop bool), error) {
	var dialAttempted bool
	for {
		c.mu.Lock()
		if conn, release := c.tryReserveLocked(addr); conn != nil {
			c.mu.Unlock()
			if err := c.verifyCachedConn(conn); err != nil {
				release(true)
				continue
			}
			conn.resetDeadlines()
			return conn, release, nil
		}
		canGrow := c.maxPerAddr <= 0 || len(c.conns[addr]) < c.maxPerAddr
		if !canGrow {
			c.cond.Wait()
			c.mu.Unlock()
			continue
		}
		c.mu.Unlock()

		dialer := &net.Dialer{}
		if c.dialTimeout > 0 {
			dialer.Timeout = c.dialTimeout
		}
		netConn, err := dialer.Dial("tcp", addr)
		if err != nil {
			return nil, nil, err
		}
		cached := &cachedConn{
			conn:  netConn,
			enc:   json.NewEncoder(netConn),
			dec:   json.NewDecoder(bufio.NewReader(netConn)),
			alive: true,
		}
		c.mu.Lock()
		cached.inUse = true
		c.conns[addr] = append(c.conns[addr], cached)
		release := c.makeReleaseLocked(addr, cached)
		c.mu.Unlock()
		if err := c.verifyCachedConn(cached); err != nil {
			release(true)
			if dialAttempted {
				return nil, nil, err
			}
			dialAttempted = true
			continue
		}
		cached.resetDeadlines()
		return cached, release, nil
	}
}

func (c *connCache) verifyCachedConn(conn *cachedConn) error {
	if conn == nil {
		return fmt.Errorf("cached connection is nil")
	}
	conn.resetDeadlines()
	var deadlineSet bool
	if c != nil && c.dialTimeout > 0 && conn.conn != nil {
		if err := conn.conn.SetDeadline(time.Now().Add(c.dialTimeout)); err != nil {
			return err
		}
		deadlineSet = true
	}
	err := conn.ping()
	if deadlineSet {
		conn.resetDeadlines()
	}
	return err
}

func (c *connCache) tryReserveLocked(addr string) (*cachedConn, func(drop bool)) {
	pool := c.conns[addr]
	for i := 0; i < len(pool); i++ {
		conn := pool[i]
		if conn == nil {
			continue
		}
		if !conn.alive {
			c.removeConnLocked(addr, conn)
			i--
			continue
		}
		if conn.inUse {
			continue
		}
		conn.inUse = true
		return conn, c.makeReleaseLocked(addr, conn)
	}
	return nil, nil
}

func (c *connCache) makeReleaseLocked(addr string, conn *cachedConn) func(drop bool) {
	return func(drop bool) {
		c.mu.Lock()
		defer func() {
			c.cond.Broadcast()
			c.mu.Unlock()
		}()
		if drop || !conn.alive {
			if conn.alive {
				conn.alive = false
				_ = conn.conn.Close()
			}
			c.removeConnLocked(addr, conn)
			return
		}
		conn.inUse = false
	}
}

func (c *connCache) removeConnLocked(addr string, target *cachedConn) {
	pool := c.conns[addr]
	for i, conn := range pool {
		if conn == target {
			pool = append(pool[:i], pool[i+1:]...)
			if len(pool) == 0 {
				delete(c.conns, addr)
			} else {
				c.conns[addr] = pool
			}
			return
		}
	}
}

func (c *connCache) CloseAll() {
	c.mu.Lock()
	conns := make([]*cachedConn, 0)
	for addr, pool := range c.conns {
		_ = addr
		for _, conn := range pool {
			conns = append(conns, conn)
		}
		delete(c.conns, addr)
	}
	c.mu.Unlock()
	for _, conn := range conns {
		if conn == nil {
			continue
		}
		if conn.alive {
			conn.alive = false
			_ = conn.conn.Close()
		}
	}
}
