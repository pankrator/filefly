Always update the docker-compose file so that it reflects any changes added by a Pull request.

## Code design
* Try to use structs and make the code reusable as much as possible by creating structs
* Use also interfaces to abstract away

## Linting and formatting
* Run `golangci-lint run` before sending a PR and fix every reported issue.
* Keep the Go code formatted; `gofmt`/`goimports` (also run by `golangci-lint`) should produce zero diffs.
