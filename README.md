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
  uiserver          # binary that serves the browser UI and REST API by calling the metadata/data servers
internal/
  dataserver        # TCP server implementation for blocks
  metadataserver    # metadata server implementation
  protocol          # shared wire types
ui/
  static            # browser UI embedded in the uiserver binary
```

## Running the servers

In two terminals run a data server and the metadata server (which references the
first server via the `--data-servers` flag). Passing `--metadata-server` to the
data server makes it periodically ping the metadata (name) server so it can
re-register itself after restarts:

```bash
go run ./cmd/dataserver --addr :9001 --storage_dir /tmp/filefly-blocks --metadata-server :9000
```

```bash
go run ./cmd/metadataserver --addr :9000 --block-size 8 --data-servers :9001
```

To serve the browser UI from the same origin as the JSON API, start the UI
server and point it at the metadata server's TCP address (the UI process talks
to both the metadata server and the data servers on behalf of the browser):

```bash
go run ./cmd/uiserver --addr :8090 --metadata-server localhost:9000
```

Pass `--download-concurrency` to control how many blocks the UI server retrieves
from the data servers at the same time when downloading files.

Visit `http://localhost:8090` to interact with the UI. Browser requests to
`/api/*` are handled by the UI server, which fetches metadata from the metadata
server and streams payloads to and from the data servers.

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
is as simple as deleting that file from the filesystem. Once every block has
been stored successfully call `complete_file` with the same metadata document to
mark the upload as finished:

```bash
cat <<'REQ' | nc localhost 9000
{"command":"complete_file","metadata":{"name":"hello.txt","total_size":8,"replicas":1,"blocks":[{"id":"hello.txt-0","size":8,"replicas":[{"data_server":":9001"}]}]}}
REQ
```

Until the completion call the metadata server keeps the plan transient so the
file does not appear in listings before its data is fully replicated.

## Uploading files with the CLI client

The `cmd/client` helper automates the metadata and data server interactions. It
requests an upload plan from the metadata server, pushes each chunk to the data
servers listed in that plan, and only then finalizes the metadata entry with
`complete_file`.

```bash
go run ./cmd/client --metadata-server :9000 --file ./hello.txt
```

The remote file name defaults to the base name of the local path. Use
`--name=my-remote-name` to override it. Replicate each block across multiple
data servers with `--replicas=<n>` (the default is one replica per block). Each
replica is written to a distinct data server, so the requested count cannot
exceed the number of configured servers.

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

## Deleting files

Remove a file from the cluster and its metadata entry by issuing the
`delete_file` command. The metadata server removes the record and instructs
each data server replica to delete its block files:

```bash
cat <<'REQ' | nc localhost 9000
{"command":"delete_file","file_name":"hello.txt"}
REQ
```

The browser UI also exposes a **Delete** button in the file details panel,
which calls the same API and refreshes the file list after the blocks are
removed.

## Configuration

* **block-size** – size of the chunks the metadata server creates when building
  upload plans.
* **data-servers** – comma-separated list of `host:port` pairs for data servers.
  The metadata server picks targets in a simple round-robin order.
* **metadata-server** – data server flag pointing at the metadata server so the
  data node can re-register automatically after interruptions.
* **metadata-ping-interval** – data server flag that controls how often the
  metadata server is pinged for registration (defaults to 15 seconds).
* **storage_dir** – directory used by each data server process to persist block
  files. Defaults to `./blocks` when running `cmd/dataserver`.
* **metadata-file** – optional path where the metadata server periodically writes
  a JSON snapshot of its in-memory state so uploads survive restarts.
* **persist-interval** – duration between metadata snapshots. Set to `0s` to
  disable automatic persistence.
* **download-concurrency** – UI server flag that limits how many block replicas
  are fetched in parallel while reconstructing a file.

This simple implementation was built for educational purposes. It should be
straightforward to extend the servers with authentication, replication, or other
production-oriented features.
