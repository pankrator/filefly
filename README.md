# FileFly

FileFly is a tiny Golang project that showcases a metadata service that
coordinates multiple TCP data servers. The metadata server keeps a catalog of
files and returns a plan that explains how a payload should be chunked and which
data server should receive each block. Separate client-side code is responsible
for pushing the blocks to the data servers using that plan. Data servers store
each block as its own file on disk so their contents survive process restarts
and can be inspected directly if needed.

The two servers communicate with a minimal JSON protocol that can easily be
inspected with tools such as `nc` or `socat`.

## Project layout

```
cmd/
  dataserver        # binary that stores raw blocks on disk
  metadataserver    # binary that stores metadata and produces upload plans
  client            # helper CLI that uploads files using the metadata plan
internal/
  dataserver        # TCP server implementation for blocks
  metadataserver    # metadata server implementation
  protocol          # shared wire types
```

## Running the servers

In two terminals run a data server and the metadata server (which references the
first server via the `--data-servers` flag):

```bash
go run ./cmd/dataserver --addr :9001 --storage_dir /tmp/filefly-blocks
```

```bash
go run ./cmd/metadataserver --addr :9000 --block-size 8 --data-servers :9001
```

## Running everything with Docker Compose

The included `docker-compose.yml` spins up two data servers and one metadata
server backed by the official `golang:1.21` image. Each data server keeps its
blocks inside a persistent named volume so files survive container restarts.

```bash
docker compose up
```

This command exposes the metadata server on `localhost:9100` and the data
servers on `localhost:9101` and `localhost:9102`. Shut everything down (and
remove the block volumes) with `docker compose down -v`.

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
correct payload for each chunk. Blocks written to the data server are persisted
as individual files below the configured storage directory, so removing a block
is as simple as deleting that file from the filesystem.

## Uploading a file with the Go client

Instead of issuing raw TCP commands, you can rely on the included Go client to
ask the metadata server for a plan and push each chunk to the referenced data
servers.

1. Make sure the servers are running (either manually or via `docker compose`).
2. Create a sample file: `echo 'hello from FileFly' > hello.txt`.
3. Upload it through the client:

   ```bash
   go run ./cmd/client --metadata-addr localhost:9100 --file ./hello.txt
   ```

   Override the stored file name with `--file-name` if you do not want to reuse
   the file's basename. The client logs each block upload as it progresses.

The CLI only sends bytes to the data servers after obtaining the plan from the
metadata server, mirroring how a production client would coordinate uploads.

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
* **storage_dir** – directory used by each data server process to persist block
  files. Defaults to `./blocks` when running `cmd/dataserver`.

This simple implementation was built for educational purposes. It should be
straightforward to extend the servers with authentication, replication, or other
production-oriented features.
