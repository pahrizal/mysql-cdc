#!/bin/bash
# Build script for creating static binary compatible with Alpine Linux

set -e

# Build for Linux AMD64 (Alpine Linux)
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o mysql-cdc-linux-amd64 .

echo "Static binary built: mysql-cdc-linux-amd64"
echo "This binary is compatible with Alpine Linux and other Linux distributions"

