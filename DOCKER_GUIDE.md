# Docker Compose Guide

This document explains how to boot FileFly with Docker Compose and upload a file
from your local machine into one of the running data server containers.

## Prerequisites

* Docker Engine 20.10+
* The Docker Compose plugin (`docker compose`)

## Starting the stack

1. From the repository root run the stack in the background:
   ```bash
   docker compose up -d
   ```
2. Confirm that the four services are healthy:
   ```bash
   docker compose ps
   ```
   The `metadata`, `dataserver1`, `dataserver2`, and `ui` services should all
   report a `running` state.
3. Tail the metadata logs (optional) to observe requests and snapshot activity:
   ```bash
   docker compose logs -f metadata
   ```
   The server periodically reports `metadata persistence` messages when it flushes
   its in-memory state to the `/metadata/metadata.json` file mounted from the
   `metadata` volume.
4. When you are finished, stop and clean up the stack:
   ```bash
   docker compose down -v
   ```

### Metadata persistence configuration

The metadata server now keeps an on-disk snapshot so the cluster can recover file
plans after a restart. The Docker Compose service wires two CLI flags into the
server to enable the feature:

* `--metadata-file /metadata/metadata.json` – points at a file stored on the
  dedicated Docker volume named `metadata`.
* `--persist-interval 30s` – instructs the server to save a fresh snapshot every
  30 seconds.

If you need to tweak these values (for example to slow down snapshotting in a
test environment) adjust the `metadata` service command in `docker-compose.yml`.
The metadata service loads the snapshot file before it accepts requests, so you
can safely restart the stack without losing uploaded file metadata.

## Uploading a local file to a data server

With the stack running, the containers expose the following ports on your local
machine:

* Metadata server: `localhost:9100`
* Data server 1: `localhost:9101`
* Data server 2: `localhost:9102`
* UI server + static assets: `localhost:8090`

The `ui` container embeds the web assets and handles `/api/*` requests itself by
calling the metadata server (for plans and metadata) and the data servers (for
uploads/downloads), so the browser can talk to the entire stack from the same
origin.

Follow these steps to push a file stored on your machine into the cluster using
the provided CLI client, which automates the metadata and data server
interactions:

1. Make sure the Go toolchain is installed on your machine (Go 1.21 matches the
   containers).
2. From the repository root, run the client and point it at the metadata server
   that Docker Compose exposed:
   ```bash
   go run ./cmd/client --metadata-server localhost:9100 --file ./path/to/local-file.txt
   ```
3. The client retrieves an upload plan from the metadata server and then streams
   the file blocks to the referenced data servers (`localhost:9101` and
   `localhost:9102`). Successful uploads are confirmed in the command output.
4. Inspect the persisted blocks by opening a shell inside one of the data server
   containers (replace `dataserver1` with `dataserver2` as needed):
   ```bash
   docker compose exec dataserver1 ls /blocks
   ```
   Each block is stored as its own file, so you can remove uploads by deleting
   the corresponding block files within the container.

If you prefer to perform the upload manually, you can still talk directly to the
TCP ports exposed by Docker Compose by using tools such as `nc` against
`localhost:9100` (metadata) followed by `localhost:9101` or `localhost:9102`
(data servers).

## Generating checksums for existing block data

Deployments that already contained block files before the CRC support was added
need to create checksum sidecar files so the data servers can validate reads.
The Compose stack ships with a helper service that mounts both block volumes and
runs the migrator against each directory:

```bash
docker compose run --rm checksum_migrator
```

The command is safe to re-run. It only writes checksum files that do not already
exist and reports a summary once both volumes have been processed. Pass
`--force` arguments in the `command` section of the service if you need to
regenerate every checksum from scratch.
