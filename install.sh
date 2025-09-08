#!/bin/bash

set -e

# Configuration
VERSION="0.0.1"
REPO_OWNER="litebase"
REPO_NAME="litebase"
INSTALL_DIR="/usr/local/bin"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_success() {
    echo -e "${GREEN}✔️ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Determine the operating system and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux*)
        OS_TYPE="linux"
        case "$ARCH" in
            x86_64) ARCH_TYPE="amd64" ;;
            i386|i686) ARCH_TYPE="386" ;;
            aarch64|arm64) ARCH_TYPE="arm64" ;;
            armv7l) ARCH_TYPE="arm" ;;
            *) 
                print_error "Unsupported architecture: $ARCH"
                exit 1
                ;;
        esac
        FILE_EXT="tar.gz"
        ;;
    Darwin*)
        OS_TYPE="darwin"
        case "$ARCH" in
            x86_64) ARCH_TYPE="amd64" ;;
            arm64) ARCH_TYPE="arm64" ;;
            *) 
                print_error "Unsupported architecture: $ARCH"
                exit 1
                ;;
        esac
        FILE_EXT="tar.gz"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        OS_TYPE="windows"
        case "$ARCH" in
            x86_64) ARCH_TYPE="amd64" ;;
            i386|i686) ARCH_TYPE="386" ;;
            *) 
                print_error "Unsupported architecture: $ARCH"
                exit 1
                ;;
        esac
        FILE_EXT="zip"
        INSTALL_DIR="/c/Windows/System32"
        ;;
    *)
        print_error "Unsupported operating system: $OS"
        exit 1
        ;;
esac

# Construct download URL
FILENAME="litebase_${VERSION}_${OS_TYPE}_${ARCH_TYPE}.${FILE_EXT}"
DOWNLOAD_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}/releases/download/v${VERSION}/${FILENAME}"

print_info "Detected OS: $OS_TYPE"
print_info "Detected Architecture: $ARCH_TYPE"
print_info "Download URL: $DOWNLOAD_URL"

# Check for required tools
check_dependencies() {
    local missing_deps=()
    
    if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
        missing_deps+=("curl or wget")
    fi
    
    if [ "$FILE_EXT" = "tar.gz" ] && ! command -v tar >/dev/null 2>&1; then
        missing_deps+=("tar")
    fi
    
    if [ "$FILE_EXT" = "zip" ] && ! command -v unzip >/dev/null 2>&1; then
        missing_deps+=("unzip")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Missing required dependencies: ${missing_deps[*]}"
        print_info "Please install the missing dependencies and try again."
        exit 1
    fi
}

# Download file using curl or wget
download_file() {
    local url="$1"
    local output="$2"
    
    print_info "Downloading Litebase CLI from: $url"
    
    if command -v curl >/dev/null 2>&1; then
        if curl -L --fail --progress-bar "$url" -o "$output"; then
            print_success "Download completed successfully"
        else
            print_error "Download failed with curl"
            return 1
        fi
    elif command -v wget >/dev/null 2>&1; then
        if wget --progress=bar:force "$url" -O "$output"; then
            print_success "Download completed successfully"
        else
            print_error "Download failed with wget"
            return 1
        fi
    else
        print_error "Neither curl nor wget is available"
        return 1
    fi
}

# Extract downloaded file
extract_file() {
    local file="$1"
    local extract_dir="$2"
    
    mkdir -p "$extract_dir"
    
    case "$FILE_EXT" in
        tar.gz)
            if tar -xzf "$file" -C "$extract_dir"; then
                print_success "Extraction completed successfully"
            else
                print_error "Failed to extract tar.gz file"
                return 1
            fi
            ;;
        zip)
            if unzip -q "$file" -d "$extract_dir"; then
                print_success "Extraction completed successfully"
            else
                print_error "Failed to extract zip file"
                return 1
            fi
            ;;
        *)
            print_error "Unsupported file extension: $FILE_EXT"
            return 1
            ;;
    esac
}

# Install binary
install_binary() {
    local binary_path="$1"
    local install_path="$2"
    
    if [ ! -f "$binary_path" ]; then
        print_error "Binary not found at: $binary_path"
        return 1
    fi
    
    # Make binary executable
    chmod +x "$binary_path"
    
    # Create install directory if it doesn't exist
    if [ "$OS_TYPE" != "windows" ]; then
        sudo mkdir -p "$(dirname "$install_path")"
        if sudo mv "$binary_path" "$install_path"; then
            print_success "Litebase CLI installed to: $install_path"
        else
            print_error "Failed to install binary to: $install_path"
            return 1
        fi
    else
        mkdir -p "$(dirname "$install_path")" 2>/dev/null || true
        if mv "$binary_path" "$install_path"; then
            print_success "Litebase CLI installed to: $install_path"
        else
            print_error "Failed to install binary to: $install_path"
            return 1
        fi
    fi
}

# Main installation process
main() {
    echo ""
    print_info "Starting Litebase CLI installation..."
    echo ""
    
    # Check dependencies
    check_dependencies
    
    # Create temporary directory
    TMP_DIR=$(mktemp -d)
    trap "rm -rf $TMP_DIR" EXIT
    
    DOWNLOAD_FILE="$TMP_DIR/$FILENAME"
    EXTRACT_DIR="$TMP_DIR/extract"
    
    # Download the file
    if ! download_file "$DOWNLOAD_URL" "$DOWNLOAD_FILE"; then
        print_error "Installation failed during download"
        exit 1
    fi
    
    # Extract the file
    if ! extract_file "$DOWNLOAD_FILE" "$EXTRACT_DIR"; then
        print_error "Installation failed during extraction"
        exit 1
    fi
    
    # Find the binary in the extracted files
    BINARY_NAME="litebase"
    if [ "$OS_TYPE" = "windows" ]; then
        BINARY_NAME="litebase.exe"
    fi
    
    BINARY_PATH=$(find "$EXTRACT_DIR" -name "$BINARY_NAME" -type f | head -1)
    
    if [ -z "$BINARY_PATH" ]; then
        print_error "Binary '$BINARY_NAME' not found in extracted files"
        exit 1
    fi
    
    # Install the binary
    INSTALL_PATH="$INSTALL_DIR/$BINARY_NAME"
    if ! install_binary "$BINARY_PATH" "$INSTALL_PATH"; then
        print_error "Installation failed"
        exit 1
    fi
    
    echo ""
    print_success "Installation completed successfully!"
    
    # Verify installation
    echo ""
    print_info "Verifying installation..."
    
    if command -v litebase >/dev/null 2>&1; then
        print_success "Litebase CLI is available in PATH"
        echo ""
        print_info "Installed version:"
        litebase --version 2>/dev/null || echo "Version information not available"
    else
        print_warning "Litebase CLI may not be available in PATH"
        print_info "You may need to add $INSTALL_DIR to your PATH environment variable"
        print_info "Or run the binary directly from: $INSTALL_PATH"
    fi
    
    echo ""
    print_info "Setup complete! Run 'litebase --help' to get started."
    echo ""
}

# Run main function
main "$@"

