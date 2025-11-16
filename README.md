# FileFly

FileFly is a tiny Golang project that showcases a metadata service that
coordinates multiple TCP data servers. The metadata server keeps a catalog of
files and returns a plan that explains how a payload should be chunked and which
data server should receive each block. Separate client-side code is responsible
for pushing the blocks to the data servers using that plan. Data servers only
store opaque block data in-memory.

The two servers communicate with a minimal JSON protocol that can easily be
inspected with tools such as `nc` or `socat`.

## Project layout

```
cmd/
  dataserver        # binary that stores raw blocks in memory
  metadataserver    # binary that stores metadata and produces upload plans
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

## Planning uploads for a file

All commands use JSON documents delimited by newlines. The `store_file` command
accepts either a base64 payload (`data`) or just the file size (`file_size`).
The metadata server uses this information to chunk the file and returns a plan
that lists which data server should host each block. The caller must then push
the actual block bytes to the data servers.

```bash
cat <<'REQ' | nc localhost 9000
{"command":"store_file","file_name":"hello.txt","file_size":25}
REQ
```

The response includes metadata describing the data servers chosen for each
block. To upload the chunks the client can issue `store` commands directly to
each referenced data server:

```bash
cat <<'REQ' | nc localhost 9001
{"command":"store","block_id":"hello.txt-0","data":"$(printf 'blockone' | base64)"}
REQ
```

Repeat this for each block listed in the metadata response, substituting the
correct payload for each chunk.

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

* **block-size** – size of the chunks the metadata server creates when building
  upload plans.
* **data-servers** – comma-separated list of `host:port` pairs for data servers.
  The metadata server picks targets in a simple round-robin order.

This simple implementation keeps everything in memory and was built for
educational purposes. It should be straightforward to extend the servers with
persistence or authentication if desired.
