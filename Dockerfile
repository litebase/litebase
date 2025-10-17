FROM golang:1.25 AS builder

# Accept VERSION as a build argument
ARG VERSION=dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the static Go binary
RUN CGO_ENABLED=1 go build -o litebase -tags=production -ldflags="-s -w -X 'main.Version=$VERSION'" ./cmd/litebase

FROM debian:bookworm-slim

# Install required runtime dependencies for CGO/SQLite
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Security: Create a dedicated, non-root user (appuser) and group (appgroup).
# This prevents the application from running with elevated permissions.
RUN groupadd --system appgroup && useradd --system --gid appgroup appuser

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/litebase /app/litebase

# Ensure the non-root user owns the binary (important if the app needs to write to its working dir)
RUN chown -R appuser:appgroup /app && \
    chmod -R 755 /app

# Create home directory for appuser with proper permissions
RUN mkdir -p /home/appuser && chown -R appuser:appgroup /home/appuser && chmod 755 /home/appuser

# Switch to the non-root user
USER appuser

# Expose the application port
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["/app/litebase"]
CMD ["start"]
