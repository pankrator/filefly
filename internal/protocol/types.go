package protocol

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
}

// MetadataRequest is consumed by the metadata server.
type MetadataRequest struct {
	Command  string `json:"command"`
	FileName string `json:"file_name,omitempty"`
	Data     string `json:"data,omitempty"`
	FileSize int    `json:"file_size,omitempty"`
}

// BlockRef is the metadata for a block.
type BlockRef struct {
	ID         string `json:"id"`
	DataServer string `json:"data_server"`
	Size       int    `json:"size"`
}

// FileMetadata keeps track of how a file is distributed among data servers.
type FileMetadata struct {
	Name      string     `json:"name"`
	TotalSize int        `json:"total_size"`
	Blocks    []BlockRef `json:"blocks"`
}

// MetadataResponse is returned by the metadata server.
type MetadataResponse struct {
	Status   string        `json:"status"`
	Error    string        `json:"error,omitempty"`
	Metadata *FileMetadata `json:"metadata,omitempty"`
	Data     string        `json:"data,omitempty"`
}
