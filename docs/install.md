# Litebase Installation Guide

## Quick Installation

### One-line Installation (Recommended)

**For all platforms** (macOS, Linux, Windows with Git Bash/WSL):

```bash
curl -fsSL https://raw.githubusercontent.com/litebase/litebase/main/install.sh | bash
```

### Alternative Installation Methods

#### Using wget

```bash
wget -qO- https://raw.githubusercontent.com/litebase/litebase/main/install.sh | bash
```

#### Manual Download and Execute

```bash
# Download the script
curl -fsSL https://raw.githubusercontent.com/litebase/litebase/main/install.sh -o install.sh

# Make it executable
chmod +x install.sh

# Run the installation
./install.sh
```

## System Requirements

### Dependencies

The installation script automatically checks for and requires the following tools:

- **curl** or **wget** (for downloading)
- **tar** (for extracting .tar.gz files on Linux/macOS)
- **unzip** (for extracting .zip files on Windows)

### Supported Platforms

| Operating System | Architectures                     | Status      |
| ---------------- | --------------------------------- | ----------- |
| Linux            | x86_64, i386, arm64, armv7        | ✅ Supported |
| macOS            | x86_64, **arm64** (Apple Silicon) | ✅ Supported |
| Windows          | x86_64, i386                      | ✅ Supported |

## What the Installation Script Does

1. **Detects your operating system and architecture**
2. **Downloads the appropriate binary** from GitHub releases
3. **Extracts the binary** from the archive
4. **Installs the binary** to a system directory:
   - Linux/macOS: `/usr/local/bin/litebase`
   - Windows: `/c/Windows/System32/litebase.exe`
5. **Verifies the installation** by checking if the binary is accessible

## Installation Locations

### Linux and macOS

- **Installation directory**: `/usr/local/bin/`
- **Requires**: `sudo` privileges for system-wide installation
- **PATH**: Usually already included in system PATH

### Windows

- **Installation directory**: `C:\Windows\System32\`
- **PATH**: Automatically available system-wide

## Troubleshooting

### Permission Issues

If you encounter permission issues on Linux/macOS:

```bash
# The script will automatically request sudo privileges when needed
# Make sure your user has sudo access
```

### Binary Not Found in PATH

If `litebase` command is not found after installation:

**Linux/macOS:**

```bash
# Check if /usr/local/bin is in your PATH
echo $PATH

# If not, add it to your shell profile
echo 'export PATH="/usr/local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

**Windows:**

```bash
# Check if the binary exists
ls /c/Windows/System32/litebase.exe

# If it exists but command not found, check your PATH
echo $PATH
```

### Download Issues

If the download fails:

1. **Check your internet connection**
2. **Verify the release exists** on GitHub
3. **Try using wget instead of curl**:

   ```bash
   wget -qO- https://raw.githubusercontent.com/litebase/litebase/main/install.sh | bash
   ```

### Architecture Not Supported

If you get an "Unsupported architecture" error:

1. **Check your architecture**: `uname -m`
2. **Check available releases** on the GitHub releases page
3. **Consider building from source** if your architecture isn't supported

## Manual Installation

If the automated script doesn't work for your system, you can install manually:

1. **Download the appropriate binary** from [GitHub releases](https://github.com/litebase/litebase/releases)
2. **Extract the archive**:

   ```bash
   # For .tar.gz files
   tar -xzf litebase_*.tar.gz
   
   # For .zip files
   unzip litebase_*.zip
   ```

3. **Move the binary** to a directory in your PATH:

   ```bash
   # Linux/macOS
   sudo mv litebase /usr/local/bin/
   
   # Windows
   mv litebase.exe /c/Windows/System32/
   ```

4. **Make it executable** (Linux/macOS):

   ```bash
   chmod +x /usr/local/bin/litebase
   ```

## Verification

After installation, verify that Litebase is working:

```bash
# Check version
litebase --version

# Show help
litebase --help
```

## Uninstallation

To uninstall Litebase:

```bash
# Linux/macOS
sudo rm /usr/local/bin/litebase

# Windows
rm /c/Windows/System32/litebase.exe
```

## Getting Help

If you encounter issues:

1. **Check this troubleshooting guide**
2. **Search existing issues** on GitHub
3. **Create a new issue** with details about your system and the error message
4. **Join our community** for real-time help

## Security Notes

- The installation script downloads binaries from official GitHub releases
- Always verify the script content before running: `curl -fsSL https://raw.githubusercontent.com/litebase/litebase/main/install.sh`
- The script requires `sudo` privileges on Linux/macOS for system-wide installation
