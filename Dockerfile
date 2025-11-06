# Build arguments for cross-platform builds
ARG TARGETOS

# Builder stage - uses golang base image that supports multi-OS
# We use the target platform directly for CGO compatibility
FROM golang:1.25 AS builder

# Accept VERSION as a build argument
ARG VERSION=dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the Go binary for the target platform
# CGO_ENABLED=1 is required for SQLite
RUN CGO_ENABLED=1 go build -o litebase -tags=production \
    -ldflags="-s -w -X 'main.Version=$VERSION'" \
    ./cmd/litebase

# Linux runtime stage
FROM debian:bookworm-slim AS runtime-linux

# Install required runtime dependencies for CGO/SQLite
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Security: Create a dedicated, non-root user (appuser) and group (appgroup).
RUN groupadd --system appgroup && useradd --system --gid appgroup appuser

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/litebase /app/litebase

# Ensure the non-root user owns the binary
RUN chown -R appuser:appgroup /app && \
    chmod -R 755 /app

# Create home directory for appuser with proper permissions
RUN mkdir -p /home/appuser && chown -R appuser:appgroup /home/appuser && chmod 755 /home/appuser

# Switch to the non-root user
USER appuser

# Command to run the application
ENTRYPOINT ["/app/litebase"]
CMD ["start"]

# Windows runtime stage
FROM mcr.microsoft.com/windows/nanoserver:ltsc2025 AS runtime-windows

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/litebase /app/litebase.exe

# Windows containers don't support USER directive in the same way
# The application will run as ContainerUser by default in nanoserver

# Command to run the application
ENTRYPOINT ["C:\\app\\litebase.exe"]
CMD ["start"]

# Final stage - select the appropriate runtime based on target OS
FROM runtime-${TARGETOS} AS final
