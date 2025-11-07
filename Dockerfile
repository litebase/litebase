# Build arguments for cross-platform builds
ARG TARGETOS
ARG TARGETARCH

# Linux runtime stage
FROM debian:bookworm-slim AS runtime-linux

# Install required runtime dependencies for CGO/SQLite
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Security: Create a dedicated, non-root user (litebase) and group (litebase).
RUN groupadd --system litebase && useradd --system --gid litebase litebase

WORKDIR /app

# Accept TARGETARCH to determine which pre-built binary to copy
ARG TARGETARCH

# Copy the pre-built Linux binary based on architecture
COPY ./extracted-binaries/linux/litebase-${TARGETARCH} /app/litebase

# Ensure the non-root user owns the binary
RUN chown -R litebase:litebase /app && \
    chmod -R 755 /app

# Create home directory for litebase with proper permissions
RUN mkdir -p /home/litebase && chown -R litebase:litebase /home/litebase && chmod 755 /home/litebase

# Switch to the non-root user
USER litebase

# Command to run the application
ENTRYPOINT ["/app/litebase"]
CMD ["start"]

# Windows runtime stage
FROM mcr.microsoft.com/windows/nanoserver:ltsc2025 AS runtime-windows

WORKDIR /app

# Accept TARGETARCH to determine which pre-built binary to copy
ARG TARGETARCH

# Copy the pre-built Windows binary
COPY ./extracted-binaries/windows/litebase-${TARGETARCH}.exe /app/litebase.exe

# Command to run the application
ENTRYPOINT ["C:\\app\\litebase.exe"]
CMD ["start"]

# Final stage - select the appropriate runtime based on target OS
FROM runtime-${TARGETOS} AS final
