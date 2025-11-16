package protocol

import "time"

// DataServerRequest represents a message sent to a data server for storing or
// retrieving a block.
type DataServerRequest struct {
	Command string `json:"command"`
	BlockID string `json:"block_id"`
	Data    string `json:"data,omitempty"`
}

// DataServerResponse is returned by the data server.
type DataServerResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Data   string `json:"data,omitempty"`
	Pong   bool   `json:"pong,omitempty"`
}

// MetadataRequest is consumed by the metadata server.
type MetadataRequest struct {
	Command        string        `json:"command"`
	FileName       string        `json:"file_name,omitempty"`
	Data           string        `json:"data,omitempty"`
	FileSize       int           `json:"file_size,omitempty"`
	Replicas       int           `json:"replicas,omitempty"`
	Metadata       *FileMetadata `json:"metadata,omitempty"`
	DataServerAddr string        `json:"data_server_addr,omitempty"`
}

// BlockReplica describes a copy of a block stored on a specific data server.
type BlockReplica struct {
	DataServer string `json:"data_server"`
}

// BlockRef is the metadata for a block.
type BlockRef struct {
	ID       string         `json:"id"`
	Size     int            `json:"size"`
	Replicas []BlockReplica `json:"replicas"`
	// DataServer is deprecated but kept for backward compatibility with
	// persisted metadata written before replication support.
	DataServer string `json:"data_server,omitempty"`
}

// FileMetadata keeps track of how a file is distributed among data servers.
type FileMetadata struct {
	Name      string     `json:"name"`
	TotalSize int        `json:"total_size"`
	Blocks    []BlockRef `json:"blocks"`
	Replicas  int        `json:"replicas"`
}

// MetadataResponse is returned by the metadata server.
type MetadataResponse struct {
	Status   string             `json:"status"`
	Error    string             `json:"error,omitempty"`
	Metadata *FileMetadata      `json:"metadata,omitempty"`
	Data     string             `json:"data,omitempty"`
	Files    []FileMetadata     `json:"files,omitempty"`
	Servers  []DataServerHealth `json:"servers,omitempty"`
}

// DataServerHealth describes the last known health of a data server.
type DataServerHealth struct {
	Address     string    `json:"address"`
	Healthy     bool      `json:"healthy"`
	LastPong    time.Time `json:"last_pong,omitempty"`
	LastChecked time.Time `json:"last_checked,omitempty"`
	Error       string    `json:"error,omitempty"`
}
