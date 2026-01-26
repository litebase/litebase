# Cluster Documentation

## Overview

Litebase's cluster architecture provides distributed database hosting with
horizontal scaling, tiered storage, and centralized resource management.

## Topics

### Resource Management

- [Memory Management](./memory_management.md) - Global memory
  coordination across components

### Core Components

- Node Management - Primary/replica election, cluster coordination
- File Systems - Tiered storage with local, network, and object tiers
- Event Broadcasting - Inter-node communication and state synchronization

## Quick Links

- [Memory Manager API](../memory/overview.md)
- [Storage Architecture](../storage/index.md)
- [Database Manager](../database/index.md)

## Architecture Principles

### 1. Shared-Nothing Architecture

Each database operates independently with no shared state between databases.
This enables:

- Horizontal scaling to millions of databases
- Isolation between tenants
- Independent resource allocation

### 2. Tiered Storage

Three storage tiers for different access patterns:

- **Local**: Fast ephemeral storage for active files
- **Network**: Shared coordination (EFS) for cluster state
- **Object**: Durable long-term storage (S3) for persistence

### 3. Primary-Replica Model

One primary node handles writes; replicas handle reads:

- Automatic failover via lease-based election
- Eventual consistency for read replicas
- WAL synchronization for durability

### 4. Global Resource Management

Centralized memory management prevents resource exhaustion:

- Fair allocation across databases
- Priority-based eviction
- Graceful degradation under pressure

## Getting Started

### Initialize Cluster

```go
import (
    "github.com/litebase/litebase/pkg/cluster"
    "github.com/litebase/litebase/pkg/config"
)

cfg := config.NewConfig()
cfg.MemoryLimit = 4 * 1024 * 1024 * 1024 // 4GB

cluster, err := cluster.NewCluster(cfg)
if err != nil {
    log.Fatal(err)
}

// Start node
<-cluster.Node().Start()

// Check if primary
if cluster.Node().IsPrimary() {
    log.Println("Running as primary node")
} else {
    log.Printf("Running as replica, primary: %s", cluster.Node().PrimaryAddress())
}
```

### Access Shared Resources

```go
// Memory Manager
memManager := cluster.MemoryManager
metrics := memManager.Metrics()

// File Systems
localFS := cluster.LocalFS()      // Instance-local storage
networkFS := cluster.NetworkFS()  // Shared cluster coordination
objectFS := cluster.ObjectFS()    // S3-compatible durable storage
tieredFS := cluster.TieredFS()    // Network → Object with caching
```

## Configuration

### Environment Variables

```bash
# Memory Management
export LITEBASE_MEMORY_LIMIT="4GB"

# Storage Paths
export LITEBASE_STORAGE_LOCAL_PATH="/data/local"
export LITEBASE_STORAGE_NETWORK_PATH="/mnt/efs/litebase"
export LITEBASE_STORAGE_TMP_PATH="/tmp/litebase"

# Object Storage (S3)
export LITEBASE_STORAGE_OBJECT_MODE="object"
export LITEBASE_STORAGE_BUCKET="litebase-production"
export LITEBASE_STORAGE_REGION="us-east-1"

# Node Configuration
export LITEBASE_PORT="8080"              # Public API port
export LITEBASE_PRIVATE_PORT="8081"       # Inter-node communication
export LITEBASE_NODE_ADDRESS_PROVIDER="ec2"  # Auto-detect EC2 IP
```

### Programmatic Configuration

```go
cfg := &config.Config{
    Port:                "8080",
    PrivatePort:         "8081",
    MemoryLimit:         4 * 1024 * 1024 * 1024,
    StorageLocalPath:    "/data/local",
    StorageNetworkPath:  "/mnt/efs/litebase",
    StorageBucket:       "litebase-production",
    StorageObjectMode:   "object",
}
```

## Monitoring

### Cluster Health

```go
// Check node status
node := cluster.Node()

fmt.Printf("Node ID: %s\n", node.ID)
fmt.Printf("Membership: %s\n", node.GetMembership())
fmt.Printf("Primary Address: %s\n", node.PrimaryAddress())

// Memory metrics
memMetrics := cluster.MemoryManager.Metrics()

fmt.Printf("Memory Usage: %d / %d (%.1f%%)\n",
    memMetrics.Used,
    memMetrics.Capacity,
    memMetrics.UsagePercent,
)
```

### Component-Level Tracking

```go
// Memory usage by component
components := cluster.MemoryManager.ComponentUsage()

for name, usage := range components {
    fmt.Printf("%s: %d bytes\n", name, usage)
}
```

## Best Practices

### 1. Set Appropriate Memory Limits

Leave headroom for OS and other processes:

```bash
# For 32GB server
export LITEBASE_MEMORY_LIMIT="24GB"
```

### 2. Monitor Primary Elections

Log primary changes to detect instability:

```go
cluster.Node().OnStarted(func() {
    if cluster.Node().IsPrimary() {
        log.Printf("Became primary node: %s", cluster.Node().ID)
    }
})
```

### 3. Handle Memory Pressure

Implement retry logic for memory exhaustion:

```go
func getConnection(
    cluster *cluster.Cluster,
    dbID,
    branchID string,
) (*Connection, error) {
    for i := 0; i < 3; i++ {
        mgr := cluster.DatabaseManager.ConnectionManager()
        conn, err := mgr.Get(dbID, branchID)

        if err == nil {
            return conn, nil
        }

        if strings.Contains(err.Error(), "insufficient memory") {
            time.Sleep(time.Duration(i+1) * time.Second)
            continue
        }

        return nil, err
    }

    return nil, errors.New("connection failed after retries")
}
```

### 4. Graceful Shutdown

Release resources properly:

```go
defer func() {
    cluster.DatabaseManager.ConnectionManager().Shutdown()
    cluster.Node().Shutdown()
    cluster.ShutdownStorage()
}()
```

## Troubleshooting

### Primary Election Issues

**Symptoms**: Frequent primary changes, split-brain

**Diagnosis**:

- Check network filesystem latency
- Verify lease file accessibility
- Review election logs

**Solutions**:

- Increase lease duration (not recommended)
- Improve network filesystem performance
- Reduce node count if network is unstable

### Memory Exhaustion

**Symptoms**: "insufficient memory" errors, high memory usage

**Diagnosis**:

```go
metrics := cluster.MemoryManager.Metrics()
components := cluster.MemoryManager.ComponentUsage()

log.Printf("Total: %d bytes (%.1f%%)", metrics.Used, metrics.UsagePercent)

for name, usage := range components {
    log.Printf("%s: %d bytes", name, usage)
}
```

**Solutions**:

- Increase `LITEBASE_MEMORY_LIMIT`
- Reduce max connections
- Enable more aggressive cache eviction
- Scale horizontally

### Storage Tier Sync Issues

**Symptoms**: Files not syncing to object storage

**Diagnosis**:

- Check if node is primary (only primary syncs)
- Review TieredFS dirty file logs
- Verify object storage credentials

**Solutions**:

- Ensure primary node has write access to object storage
- Check `CanSyncDirtyFiles()` function
- Review eviction logs for flush failures

## See Also

- [Memory Manager Overview](../memory/overview.md)
- [Storage Architecture](../storage/index.md)
- [Database Manager](../database/index.md)
- [Testing Guide](../testing/index.md)
