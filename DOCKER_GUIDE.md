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
2. Confirm that the three services are healthy:
   ```bash
   docker compose ps
   ```
   The `metadata`, `dataserver1`, and `dataserver2` services should all report a
   `running` state.
3. Tail the metadata logs (optional) to observe requests:
   ```bash
   docker compose logs -f metadata
   ```
4. When you are finished, stop and clean up the stack:
   ```bash
   docker compose down -v
   ```

## Uploading a local file to a data server

With the stack running, the containers expose the following ports on your local
machine:

* Metadata server: `localhost:9100`
* Data server 1: `localhost:9101`
* Data server 2: `localhost:9102`

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
