.PHONY: test test-e2e

# Run the full Go test suite, including the new end-to-end tests.
test:
	go test ./...

# Focus on the end-to-end tests only.
test-e2e:
	go test ./internal/e2e
