## Installation

To install the latest version of Litebase, run the following command or download one of the binaries below for manual installation.

```shell
curl -sSL https://litebase.com/install.sh | bash
```

### Manual Installation

After downloading the appropriate binary for your platform and system architecture, follow these instructions:

<details>
<summary>**Linux**</summary>
<summary>Linux</summary>

Extract the archive and install the binary:

```bash
# Extract the downloaded archive (replace with your specific version and architecture)
tar -xzf litebase-$VERSION-linux-x86_64.tar.gz

# Make the binary executable and move to system PATH
chmod +x litebase
sudo mv litebase /usr/local/bin/

# Verify installation
litebase --version
```

For ARM64 systems, use `litebase-$VERSION-linux-arm64.tar.gz` instead.
</details>

<details>
<summary>**macOS**</summary>
<summary>macOS</summary>

#### Option 1: Using the .pkg installer (Recommended)

```bash
# Download and install the .pkg file (signed and notarized)
sudo installer -pkg litebase-$VERSION-darwin-x86_64.pkg -target /

# Verify installation
litebase --version
```

For ARM64 systems, use `litebase-$VERSION-darwin-arm64.pkg` instead.

##### Option 2: Manual installation from archive

```bash
# Extract the downloaded archive (replace with your specific version and architecture)
tar -xzf litebase-$VERSION-darwin-x86_64.tar.gz

# Move to system PATH
sudo mv litebase /usr/local/bin/

# Verify installation
litebase --version
```

For ARM64 systems, use `litebase-$VERSION-darwin-arm64.tar.gz` instead.
</details>

<details>
<summary>**Windows**</summary>
<summary>Windows</summary>

Extract the archive and install the binary:

```powershell
# Extract the downloaded archive (replace with your specific version)
tar -xzf litebase-$VERSION-windows-x86_64.tar.gz

# Create directory and move binary to PATH
New-Item -ItemType Directory -Force -Path "C:\Program Files\litebase"
Move-Item litebase.exe "C:\Program Files\litebase\"

# Verify installation
litebase.exe --version
```

Alternatively, you can place `litebase.exe` in any directory and add that directory to your system's PATH environment variable.
</details>

## Security

All binaries are **code signed for authenticity and integrity verification**, and checksums are provided for additional integrity checks.

- **macOS**: Code signed with Developer ID, pkg installers are also notarized by Apple
- **Linux**: GPG signed with checksums (SHA256/SHA512)  
- **Windows**: Signed with a Trusted Identity certificate
- **All platforms**: Include SLSA build provenance attestation

Code signing ensures you're downloading genuine, unmodified binaries from the official maintainer.
