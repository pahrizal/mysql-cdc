#!/bin/bash
# Release script for mysql-cdc
# Creates individual tar.gz archives for each platform binary
# Note: GitHub automatically creates source code archives when you create a release

set -e

VERSION="${1:-v1.0.0}"
TEMP_DIR=".release-temp"
BINARY_NAME="mysql-cdc"

echo "Creating release ${VERSION}..."

# Clean up any existing release files
rm -rf "${TEMP_DIR}"
rm -f mysql-cdc-*.tar.gz *.sha256

# Create temporary directory
mkdir -p "${TEMP_DIR}"

# Function to create platform archive
create_platform_archive() {
    local os=$1
    local arch=$2
    local binary_name=$3
    local archive_name="mysql-cdc-${os}-${arch}.tar.gz"
    local platform_dir="${TEMP_DIR}/mysql-cdc-${os}-${arch}"
    
    echo "  Creating ${archive_name}..."
    
    # Remove existing platform directory if it exists
    rm -rf "${platform_dir}"
    
    # Create platform-specific directory
    mkdir -p "${platform_dir}"
    
    # Copy binary (rename to mysql-cdc with appropriate extension)
    cp "${binary_name}" "${platform_dir}/mysql-cdc${4}"  # $4 is extension (.exe for Windows)
    if [ -f "${platform_dir}/mysql-cdc${4}" ]; then
        chmod +x "${platform_dir}/mysql-cdc${4}"
    fi
        
    # Create archive (files at root level, no nested folder)
    tar -czf "${archive_name}" -C "${platform_dir}/" .
    
    # Calculate checksum
    if command -v shasum &> /dev/null; then
        shasum -a 256 "${archive_name}" > "${archive_name}.sha256"
    elif command -v sha256sum &> /dev/null; then
        sha256sum "${archive_name}" > "${archive_name}.sha256"
    fi
    
    echo "    ✓ Created ${archive_name}"
}

# Build binaries for different platforms
echo "Building binaries..."

# Linux AMD64 (static binary for Alpine Linux)
echo "  Building Linux AMD64..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o "${TEMP_DIR}/binary-linux-amd64" .
create_platform_archive "linux" "amd64" "${TEMP_DIR}/binary-linux-amd64" ""

# Linux ARM64
echo "  Building Linux ARM64..."
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -a -ldflags '-extldflags "-static"' -o "${TEMP_DIR}/binary-linux-arm64" .
create_platform_archive "linux" "arm64" "${TEMP_DIR}/binary-linux-arm64" ""

# macOS AMD64
echo "  Building macOS AMD64..."
GOOS=darwin GOARCH=amd64 go build -o "${TEMP_DIR}/binary-darwin-amd64" .
create_platform_archive "darwin" "amd64" "${TEMP_DIR}/binary-darwin-amd64" ""

# macOS ARM64 (Apple Silicon)
echo "  Building macOS ARM64..."
GOOS=darwin GOARCH=arm64 go build -o "${TEMP_DIR}/binary-darwin-arm64" .
create_platform_archive "darwin" "arm64" "${TEMP_DIR}/binary-darwin-arm64" ""

# Windows AMD64
echo "  Building Windows AMD64..."
GOOS=windows GOARCH=amd64 go build -o "${TEMP_DIR}/binary-windows-amd64.exe" .
create_platform_archive "windows" "amd64" "${TEMP_DIR}/binary-windows-amd64.exe" ".exe"

# Clean up temporary directory
rm -rf "${TEMP_DIR}"

echo ""
echo "Release ${VERSION} created successfully!"
echo ""
echo "Platform archives:"
ls -lh mysql-cdc-*.tar.gz 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "Checksum files:"
ls -lh *.sha256 2>/dev/null | awk '{print "  " $9}'
echo ""
echo "Note: GitHub will automatically create source code archives (tar.gz and zip) when you create the release."
echo ""
echo "Next steps:"
echo "1. Create and push git tag:"
echo "   git tag -a ${VERSION} -m 'Release ${VERSION}'"
echo "   git push origin ${VERSION}"
echo ""
echo "2. Upload release assets to GitHub:"
echo "   https://github.com/$(git remote get-url origin | sed -E 's/.*github.com[:/]([^/]+\/[^/]+)(\.git)?$/\1/')/releases/new"
