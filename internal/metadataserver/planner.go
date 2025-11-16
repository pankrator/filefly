package metadataserver

import (
	"fmt"
	"sync"

	"filefly/internal/protocol"
)

type replicaSelector interface {
	Validate(requested int) (int, error)
	NextSet(count int) ([]string, error)
}

type blockPlanner struct {
	blockSize int
	selector  replicaSelector
}

func newBlockPlanner(blockSize int, selector replicaSelector) *blockPlanner {
	return &blockPlanner{blockSize: blockSize, selector: selector}
}

func (p *blockPlanner) Plan(name string, totalSize, requestedReplicas int) (*protocol.FileMetadata, error) {
	if p == nil {
		return nil, fmt.Errorf("planner is nil")
	}
	if p.blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size")
	}
	replicas, err := p.selector.Validate(requestedReplicas)
	if err != nil {
		return nil, err
	}
	blocks, err := p.planBlocks(name, totalSize, replicas)
	if err != nil {
		return nil, err
	}
	meta := protocol.FileMetadata{
		Name:      name,
		TotalSize: totalSize,
		Blocks:    blocks,
		Replicas:  replicas,
	}
	return &meta, nil
}

func (p *blockPlanner) planBlocks(fileName string, totalSize, replicas int) ([]protocol.BlockRef, error) {
	if totalSize == 0 {
		return nil, nil
	}
	blocks := make([]protocol.BlockRef, 0, totalSize/p.blockSize+1)
	remaining := totalSize
	for remaining > 0 {
		chunkSize := p.blockSize
		if chunkSize > remaining {
			chunkSize = remaining
		}
		blockID := fmt.Sprintf("%s-%d", fileName, len(blocks))
		servers, err := p.selector.NextSet(replicas)
		if err != nil {
			return nil, err
		}
		replicasMeta := make([]protocol.BlockReplica, 0, len(servers))
		for _, addr := range servers {
			replicasMeta = append(replicasMeta, protocol.BlockReplica{DataServer: addr})
		}
		blocks = append(blocks, protocol.BlockRef{
			ID:       blockID,
			Size:     chunkSize,
			Replicas: replicasMeta,
		})
		remaining -= chunkSize
	}
	return blocks, nil
}

type roundRobinSelector struct {
	mu      sync.Mutex
	servers []string
	next    int
}

func newRoundRobinSelector(servers []string) *roundRobinSelector {
	copy := append([]string(nil), servers...)
	return &roundRobinSelector{servers: copy}
}

func (r *roundRobinSelector) Validate(requested int) (int, error) {
	if len(r.servers) == 0 {
		return 0, fmt.Errorf("no data servers configured")
	}
	if requested <= 0 {
		requested = 1
	}
	if requested > len(r.servers) {
		return 0, fmt.Errorf("replica count %d exceeds available data servers (%d)", requested, len(r.servers))
	}
	return requested, nil
}

func (r *roundRobinSelector) NextSet(count int) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.servers) == 0 {
		return nil, fmt.Errorf("no data servers configured")
	}
	set := make([]string, 0, count)
	for i := 0; i < count; i++ {
		addr := r.servers[r.next%len(r.servers)]
		r.next++
		set = append(set, addr)
	}
	return set, nil
}
