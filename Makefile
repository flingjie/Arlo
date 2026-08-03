.PHONY: build test lint proto clean

# Build both binaries
build:
	go build -o bin/arlod ./cmd/arlod
	go build -o bin/arlo ./cmd/arlo

# Run all tests with race detection
test:
	go test -race -count=1 ./...

# Run tests with coverage
cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run golangci-lint
lint:
	golangci-lint run ./...

# Generate protobuf code
proto:
	buf generate api/proto

# Clean build artifacts
clean:
	rm -rf bin/

# Run the daemon (dev mode)
run-daemon:
	go run ./cmd/arlod

# Run the TUI client (dev mode)
run-cli:
	go run ./cmd/arlo
