#!/bin/bash
# Release script for mysql-cdc
# Creates binaries for multiple platforms and packages them into a tar.gz archive

set -e

VERSION="${1:-v1.0.0}"
RELEASE_DIR="mysql-cdc-${VERSION}"
ARCHIVE_NAME="mysql-cdc-${VERSION}.tar.gz"

echo "Creating release ${VERSION}..."

# Clean up any existing release directory
rm -rf "${RELEASE_DIR}"
rm -f "${ARCHIVE_NAME}"

# Create release directory
mkdir -p "${RELEASE_DIR}"

# Build binaries for different platforms
echo "Building binaries..."

# Linux AMD64 (static binary for Alpine Linux)
echo "  Building Linux AMD64..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o "${RELEASE_DIR}/mysql-cdc-linux-amd64" ./cmd/mysql-cdc

# Linux ARM64
echo "  Building Linux ARM64..."
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o "${RELEASE_DIR}/mysql-cdc-linux-arm64" ./cmd/mysql-cdc

# macOS AMD64
echo "  Building macOS AMD64..."
GOOS=darwin GOARCH=amd64 go build -o "${RELEASE_DIR}/mysql-cdc-darwin-amd64" ./cmd/mysql-cdc

# macOS ARM64 (Apple Silicon)
echo "  Building macOS ARM64..."
GOOS=darwin GOARCH=arm64 go build -o "${RELEASE_DIR}/mysql-cdc-darwin-arm64" ./cmd/mysql-cdc

# Windows AMD64
echo "  Building Windows AMD64..."
GOOS=windows GOARCH=amd64 go build -o "${RELEASE_DIR}/mysql-cdc-windows-amd64.exe" ./cmd/mysql-cdc

# Copy necessary files
echo "Copying files..."
cp config.yaml "${RELEASE_DIR}/"
cp README.md "${RELEASE_DIR}/"
cp LICENSE "${RELEASE_DIR}/"

# Create a CHANGELOG or VERSION file
echo "${VERSION}" > "${RELEASE_DIR}/VERSION"

# Create archive
echo "Creating archive ${ARCHIVE_NAME}..."
tar -czf "${ARCHIVE_NAME}" "${RELEASE_DIR}"

# Calculate checksums
echo "Calculating checksums..."
if command -v shasum &> /dev/null; then
    shasum -a 256 "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256"
    echo "  SHA256: $(cat ${ARCHIVE_NAME}.sha256)"
elif command -v sha256sum &> /dev/null; then
    sha256sum "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256"
    echo "  SHA256: $(cat ${ARCHIVE_NAME}.sha256)"
fi

# Clean up release directory (optional, comment out if you want to keep it)
# rm -rf "${RELEASE_DIR}"

echo ""
echo "Release ${VERSION} created successfully!"
echo "Archive: ${ARCHIVE_NAME}"
echo "Files included:"
ls -lh "${RELEASE_DIR}/"
echo ""
echo "To create a git tag, run:"
echo "  git tag -a ${VERSION} -m 'Release ${VERSION}'"
echo "  git push origin ${VERSION}"

