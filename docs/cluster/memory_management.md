# Cluster Memory Management

## Overview

The Litebase cluster includes a centralized Memory Manager that coordinates
memory usage across all components - file systems, caches, connections, and
buffers. This prevents memory exhaustion and enables fair resource allocation
across millions of databases.

## Architecture

### Initialization

The Memory Manager is created during cluster initialization with a
configurable memory limit:

```go
func NewCluster(config *config.Config) (*Cluster, error) {
    // Create memory manager with limit from config (default 1GB)
    memoryLimit := int64(1024 * 1024 * 1024) // 1GB default

    if config.MemoryLimit > 0 {
        memoryLimit = config.MemoryLimit
    }

    memManager, err := memory.NewManager(memory.Config{
        Capacity:  memoryLimit,
        Threshold: 0.85, // Trigger eviction at 85% usage
    })

    if err != nil {
        return nil, fmt.Errorf("failed to create memory manager: %w", err)
    }

    cluster := &Cluster{
        Config:          config,
        MemoryManager:   memManager,
        // ... other fields
    }

    return cluster, nil
}
```

### Configuration

Set memory limit via environment variable or config:

```bash
# 4GB memory limit
export LITEBASE_MEMORY_LIMIT="4GB"

# 512MB memory limit  
export LITEBASE_MEMORY_LIMIT="512MB"

# Or numeric bytes
export LITEBASE_MEMORY_LIMIT="536870912"
```

The memory limit applies to:

- Database connections (256KB per connection)
- File system buffers (32KB per buffer)
- Database caches (variable size)
- Page logger buffers (variable size)

### Global Coordination

The Memory Manager is shared across all cluster components:

```go
type Cluster struct {
    Config          *config.Config
    MemoryManager   *memory.Manager  // Shared across all components
    // ... other fields
}

// Components access via cluster reference
cluster.MemoryManager.Request(...)
cluster.MemoryManager.Release(...)
cluster.MemoryManager.Metrics()
```

## Integrated Components

### 1. TieredFileSystemDriver

The tiered file system uses memory-managed buffer pools for file operations:

```go
func NewTieredFileSystemDriver(
    ctx context.Context,
    highTier *FileSystem,
    lowTier *FileSystem,
    memManager *memory.Manager,
) *TieredFileSystemDriver {
    fsd := &TieredFileSystemDriver{
        bufferPool: memory.NewBytesBufferPool(32*1024, memManager),
        memoryManager: memManager,
        // ... other fields
    }
    return fsd
}
```

**Memory Usage**:

- 32KB buffers for file I/O operations
- Buffers are reclaimable (can be evicted under pressure)
- Medium priority (below connections, above caches)

### 2. ConnectionManager

Database connections track memory usage via leases:

```go
type ConnectionManager struct {
    memoryManager  *memory.Manager
    connectionSize int64 // 256KB per connection
    // ... other fields
}

func (cm *ConnectionManager) Get(
    dbID,
    branchID string,
) (*ClientConnection, error) {
    // Request memory lease for connection
    lease, err := cm.memoryManager.Request(
        cm.connectionSize,
        memory.Reclaimable(false), // Active connections
        memory.WithPriority(memory.PriorityHigh),
        memory.WithOwner(
            fmt.Sprintf("connection-%s-%s", dbID, branchID),
        ),
    )

    if err != nil {
        return nil, fmt.Errorf("insufficient memory for connection: %w", err)
    }

    // ... create connection
    conn.memoryLease = lease
    
    return conn, nil
}
```

**Memory Usage**:

- 256KB estimated per connection
- Non-reclaimable (active connections protected)
- High priority (connections are critical)
- Graceful failure when memory exhausted

### 3. DatabaseResources (Planned)

Database caches will use ManagedCache for memory-aware storage:

```go
type DatabaseResources struct {
    manager      *memory.Manager
    databaseCache *memory.ManagedCache
    keyCache     *memory.ManagedCache
    // ... other resources
}
```

**Memory Usage**:

- Variable cache entry sizes
- Reclaimable (caches can be evicted)
- Normal priority (below connections and buffers)

### 4. PageLogger (Planned)

Page logger buffers will track memory with OnReclaim callbacks:

```go
type PageLogger struct {
    manager *memory.Manager
    lease   *memory.Lease
    // ... other fields
}

func (pl *PageLogger) AllocateBuffer(size int64) error {
    lease, err := pl.manager.Request(size,
        memory.Reclaimable(true),
        memory.WithOnReclaim(func() error {
            return pl.Flush() // Flush to disk before eviction
        }),
    )
    // ... store lease
}
```

**Memory Usage**:

- Variable buffer sizes
- Reclaimable with flush on eviction
- Normal priority

## Memory Pressure Handling

### Priority Levels

When memory is scarce, components are evicted in order:

1. **Low Priority**: Background caches, metrics
2. **Normal Priority**: Database caches, page logger buffers
3. **Medium Priority**: File system buffers
4. **High Priority**: Active database connections (protected)

### Eviction Process

1. Memory usage exceeds 85% threshold
2. Eviction goroutine activates
3. Reclaimable leases evicted by priority (low → high)
4. OnReclaim callbacks executed before eviction
5. Eviction stops when usage drops below 80%

### Failure Modes

**When memory is exhausted**:

- New connections fail with "insufficient memory" error
- Buffer requests may block or fail
- Cache puts may be rejected
- Existing connections remain active

**Client impact**:

- Connection errors trigger retry logic
- Queries queue until memory available
- No data loss or corruption
- Graceful degradation

## Monitoring

### Metrics API

```go
metrics := cluster.MemoryManager.Metrics()

fmt.Printf("Total capacity: %d bytes\n", metrics.Capacity)
fmt.Printf("Used: %d bytes (%.1f%%)\n", metrics.Used, metrics.UsagePercent)
fmt.Printf("Available: %d bytes\n", metrics.Available)
fmt.Printf("Active leases: %d\n", metrics.ActiveLeases)
fmt.Printf("Components tracked: %d\n", metrics.Components)
```

### Per-Component Tracking

```go
// Get memory usage by component
components := cluster.MemoryManager.ComponentUsage()

for name, usage := range components {
    fmt.Printf("%s: %d bytes\n", name, usage)
}
```

Example output:

```text
connection-testdb-main: 262144 bytes
connection-testdb-analytics: 262144 bytes
tiered-fs-buffers: 32768 bytes
page-logger-testdb: 1048576 bytes
Total: 1605632 bytes
```

### Health Checks

```go
health := cluster.MemoryManager.Health()

if health.Status == "critical" {
    log.Printf("Memory critically low: %s", health.Message)
    // Trigger alerts, scale up, etc.
}
```

## Best Practices

### 1. Set Appropriate Limits

```bash
# For production with 32GB RAM
export LITEBASE_MEMORY_LIMIT="24GB"  # Leave headroom for OS

# For development
export LITEBASE_MEMORY_LIMIT="2GB"
```

### 2. Monitor Usage Trends

Track memory metrics over time:

- Identify memory leaks
- Optimize cache sizes
- Plan capacity

### 3. Handle Errors Gracefully

```go
conn, err := connectionManager.Get(dbID, branchID)
if err != nil {
    if strings.Contains(err.Error(), "insufficient memory") {
        // Retry with backoff or queue request
        return handleMemoryPressure(dbID, branchID)
    }
    return err
}
```

### 4. Component-Specific Tuning

```go
// Adjust connection size estimate
connectionManager.connectionSize = 512 * 1024  // 512KB if needed

// Adjust buffer pool size
bufferPool := memory.NewBytesBufferPool(64*1024, manager)  // 64KB buffers
```

## Troubleshooting

### "insufficient memory for connection" Errors

**Symptoms**: Connection requests failing with memory errors

**Diagnosis**:

```go
metrics := cluster.MemoryManager.Metrics()
log.Printf("Memory used: %d / %d (%.1f%%)", metrics.Used, metrics.Capacity, metrics.UsagePercent)
log.Printf("Active leases: %d", metrics.ActiveLeases)
```

**Solutions**:

1. Increase memory limit: `LITEBASE_MEMORY_LIMIT="8GB"`
2. Close idle connections more aggressively
3. Reduce per-connection size estimate
4. Scale horizontally (add more nodes)

### High Buffer Memory Usage

**Symptoms**: File system buffers consuming excessive memory

**Diagnosis**:

```go
components := cluster.MemoryManager.ComponentUsage()
log.Printf("Buffer usage: %d bytes", components["tiered-fs-buffers"])
```

**Solutions**:

1. Reduce buffer size: `NewBytesBufferPool(16*1024, manager)`
2. Reduce max open files: `MaxFilesOpened = 5000`
3. Enable more aggressive buffer eviction

### Memory Leaks

**Symptoms**: Memory usage grows unbounded despite eviction

**Diagnosis**:

```go
// Track lease lifecycle
manager.SetLeaseListener(func(event LeaseEvent) {
    log.Printf("Lease %s: %s", event.Type, event.Owner)
})
```

**Solutions**:

1. Ensure all `Request()` calls have matching `Release()`
2. Check for goroutine leaks holding leases
3. Review OnReclaim callback for deadlocks
4. Enable detailed lease tracking for debugging

## Migration Guide

### Migrating Existing Components

**Before** (unbounded sync.Pool):

```go
type Component struct {
    bufferPool *sync.Pool
}

func (c *Component) getBuffer() []byte {
    return c.bufferPool.Get().([]byte)
}
```

**After** (memory-managed):

```go
type Component struct {
    bufferPool *memory.BytesBufferPool
}

func (c *Component) getBuffer() (*bytes.Buffer, error) {
    buf, err := c.bufferPool.Get()
    if err != nil {
        return nil, fmt.Errorf("failed to get buffer: %w", err)
    }
    return buf, nil
}

func (c *Component) releaseBuffer(buf *bytes.Buffer) error {
    return c.bufferPool.Put(buf)
}
```

### Testing Memory Limits

```go
func TestComponentUnderMemoryPressure(t *testing.T) {
    // Create manager with small limit
    manager, _ := memory.NewManager(memory.Config{
        Capacity:  1024 * 1024, // 1MB limit
    })

    // Test component behavior when memory exhausted
    component := NewComponent(manager)
    
    // Fill memory
    var leases []*memory.Lease
    for i := 0; i < 100; i++ {
        lease, err := manager.Request(10*1024, memory.Reclaimable(false))
        if err != nil {
            break // Expected: insufficient memory
        }
        leases = append(leases, lease)
    }

    // Verify graceful degradation
    err := component.DoWork()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "insufficient memory")
}
```

## Future Enhancements

### Planned Features

1. **Dynamic Limits**: Adjust memory limit based on available system memory
2. **Component Quotas**: Per-database or per-tenant memory limits
3. **Predictive Eviction**: Machine learning to predict memory needs
4. **Cross-Node Coordination**: Share memory state across cluster nodes
5. **Advanced Metrics**: Histogram of allocation sizes, heat maps

### API Stability

The Memory Manager API is currently **beta**. Breaking changes may occur
before v1.0:

- Lease interface may be extended
- Metrics format may change
- Configuration options may be renamed

See [Memory Manager Overview](../memory/overview.md) for detailed API
documentation.
