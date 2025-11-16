# FileFly

FileFly is a tiny Golang project that showcases a metadata service that
coordinates multiple TCP data servers. The metadata server keeps a catalog of
files, splits payloads into fixed-size blocks, and distributes blocks across the
available data servers. Data servers only store opaque block data in-memory.

The two servers communicate with a minimal JSON protocol that can easily be
inspected with tools such as `nc` or `socat`.

## Project layout

```
cmd/
  dataserver        # binary that stores raw blocks in memory
  metadataserver    # binary that stores metadata and orchestrates block writes
internal/
  dataserver        # TCP server implementation for blocks
  metadataserver    # metadata server implementation
  protocol          # shared wire types
```

## Running the servers

In two terminals run a data server and the metadata server (which references the
first server via the `--data-servers` flag):

```bash
go run ./cmd/dataserver --addr :9001
```

```bash
go run ./cmd/metadataserver --addr :9000 --block-size 8 --data-servers :9001
```

## Storing a file

All commands use JSON documents delimited by newlines. The `store_file` command
accepts a base64 payload that will be split and distributed across data
servers.

```bash
cat <<'REQ' | nc localhost 9000
{"command":"store_file","file_name":"hello.txt","data":"$(printf 'Hello distributed world!' | base64)"}
REQ
```

The response includes the metadata that records which block ended up on which
data server.

## Fetching and inspecting metadata

Fetch the file content (still base64 encoded) by sending another command to the
metadata server:

```bash
cat <<'REQ' | nc localhost 9000
{"command":"fetch_file","file_name":"hello.txt"}
REQ
```

Only metadata without the file bytes can be requested with `get_metadata`:

```bash
cat <<'REQ' | nc localhost 9000
{"command":"get_metadata","file_name":"hello.txt"}
REQ
```

## Configuration

* **block-size** – size of the chunks the metadata server creates before sending
  data to a data server.
* **data-servers** – comma-separated list of `host:port` pairs for data servers.
  The metadata server picks targets in a simple round-robin order.

This simple implementation keeps everything in memory and was built for
educational purposes. It should be straightforward to extend the servers with
persistence or authentication if desired.
