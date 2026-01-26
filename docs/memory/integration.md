# Integration Guide

## Overview

This guide shows how to integrate the Memory Manager with existing Litebase components.

## TieredFileSystemDriver Integration

### TieredFileSystemDriver Problem

The TieredFileSystemDriver currently uses raw `sync.Pool` for buffer
management with no memory tracking. With potentially 10,000+ open files per
database, memory usage can spiral out of control.

### TieredFileSystemDriver Solution

Replace the raw buffer pool with a memory-managed BufferPool:

```go
type TieredFileSystemDriver struct {
    bufferPool *memory.BufferPool
    manager    *memory.Manager
    // ... existing fields
}

func NewTieredFileSystemDriver(
    ctx context.Context,
    highTier *FileSystem,
    lowTier *FileSystem,
    manager *memory.Manager,
) *TieredFileSystemDriver {
    return &TieredFileSystemDriver{
        bufferPool: memory.NewBufferPool(
            32*1024,
            manager,
        ), // 32KB buffers
        manager:    manager,
        // ... initialize other fields
    }
}

func (fsd *TieredFileSystemDriver) CopyFile(
    dst io.Writer,
    src io.Reader,
) (int64, error) {
    buf, err := fsd.bufferPool.Get()

    if err != nil {
        return 0, fmt.Errorf("failed to get buffer: %w", err)
    }

    defer fsd.bufferPool.Put(buf)

    return io.CopyBuffer(dst, src, *buf)
}
```

### TieredFileSystemDriver Benefits

- Buffer memory is tracked globally
- Under pressure, buffers can be evicted
- No more unbounded buffer pool growth

## ConnectionManager Integration

### Problem

The ConnectionManager has unbounded connection maps with only idle timeout.
No global memory limits exist. With potentially millions of databases, the
total number of connections could easily exceed available memory.

### Solution

Request memory leases for each connection:

```go
type ConnectionManager struct {
    memoryManager  *memory.Manager
    connectionSize int64 // Estimated size per connection (256KB)
    // ... existing fields
}

type ClientConnection struct {
    Branch      *Branch
    connection  *DatabaseConnection
    memoryLease *memory.Lease // Track memory
}

func (cm *ConnectionManager) Get(
    dbID,
    branchID string,
) (*ClientConnection, error) {
    // Try to reuse existing connection
    conn := cm.findAvailableConnection(dbID, branchID)

    if conn != nil {
        return conn, nil
    }
    
    // Request memory for new connection
    var lease *memory.Lease

    if cm.memoryManager != nil {
        var err error

        lease, err = cm.memoryManager.Request(
            cm.connectionSize,
            memory.Reclaimable(false), // Can't be evicted
            memory.WithPriority(memory.PriorityHigh),
            memory.WithOwner(
                fmt.Sprintf("connection-%s-%s", dbID, branchID),
            ),
        )

        if err != nil {
            return nil, fmt.Errorf("insufficient memory for connection: %w", err)
        }
    }

    // Create new connection
    conn, err := NewClientConnection(cm, branch)

    if err != nil {
        // Release lease if connection creation fails
        if lease != nil {
            cm.memoryManager.Release(lease)
        }

        return nil, err
    }

    // Store lease in connection for later release
    conn.memoryLease = lease

    return conn, nil
}

func (cm *ConnectionManager) remove(clientConnection *ClientConnection) {
    // Release memory lease when connection is removed
    if clientConnection.memoryLease != nil && cm.memoryManager != nil {
        cm.memoryManager.Release(clientConnection.memoryLease)
        clientConnection.memoryLease = nil
    }

    clientConnection.Close()
}
```

### Implementation Details

#### Connection Size Estimation

- Each connection estimated at 256KB (SQLite connection overhead + buffers)
- Stored in `ConnectionManager.connectionSize` for easy adjustment
- Non-reclaimable to prevent evicting active connections

#### Lifecycle Management

1. **Allocation**: Request lease in `Get()` before creating connection
2. **Storage**: Lease stored in `ClientConnection.memoryLease`
3. **Release**: Lease released in `remove()` after connection closes
4. **Failure Handling**: Lease released immediately if connection creation fails

#### Memory Pressure Response

When memory is low:

- New connection requests fail with "insufficient memory" error
- Existing connections remain active (non-reclaimable)
- Idle connections removed normally via existing timeout mechanism
- Client retries or queues requests until memory available

### Initialization

Wire memory manager from cluster to connection manager:

```go
func (d *DatabaseManager) ConnectionManager() *ConnectionManager {
    d.connectionManagerMutex.Lock()
    defer d.connectionManagerMutex.Unlock()

    if d.connectionManager != nil {
        return d.connectionManager
    }

    d.connectionManager = &ConnectionManager{
        cluster:         d.Cluster,
        connectionSize:  256 * 1024,
        databaseManager: d,
        databases:       map[string]*DatabaseGroup{},
        memoryManager:   d.Cluster.MemoryManager, // Get from cluster
        mutex:           &sync.RWMutex{},
        state:           ConnectionManagerStateRunning,
    }

    // ... start connection ticker

    return d.connectionManager
}
```

### ConnectionManager Benefits

- **Hard memory limit**: Total connections capped by available memory
- **Fair allocation**: Memory distributed across all databases
- **Prevents OOM**: Connection creation fails gracefully under pressure
- **Coordinated cleanup**: Works with existing idle timeout mechanism
- **Visibility**: Track which databases are using memory for connections

## DatabaseResources Integration

### DatabaseResources Problem

DatabaseResources uses LFUCache with item-count limits, not memory limits.
Each cache is independent.

### DatabaseResources Solution

Use ManagedCache for all caches:

```go
type DatabaseResources struct {
    manager      *memory.Manager
    databaseCache *memory.ManagedCache
    keyCache     *memory.ManagedCache
    // ... other resources
}

func NewDatabaseResources(
    dbID string,
    branchID string,
    manager *memory.Manager,
) *DatabaseResources {
    return &DatabaseResources{
        manager: manager,
        databaseCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    1000,
            Manager:     manager,
            DefaultSize: 4096,
            Owner:       fmt.Sprintf("db-%s-cache", dbID),
        }),
        keyCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    5000,
            Manager:     manager,
            DefaultSize: 128,
            Owner:       fmt.Sprintf("db-%s-keys", dbID),
        }),
    }
}
```

### DatabaseResources Benefits

- Memory limits instead of arbitrary item counts
- Caches compete fairly for memory
- Component-level visibility

## PageLogger/SnapshotLogger Integration

### PageLogger Problem

Page loggers buffer writes in memory with no global coordination.

### PageLogger Solution

Track buffer memory with leases:

```go
type PageLogger struct {
    manager    *memory.Manager
    bufferSize int64
    lease      *memory.Lease
    // ... existing fields
}

func (pl *PageLogger) AllocateBuffer(size int64) error {
    lease, err := pl.manager.Request(size,
        memory.Reclaimable(true),
        memory.WithPriority(memory.PriorityNormal),
        memory.WithOwner("page-logger"),
        memory.WithOnReclaim(func() error {
            // Flush buffer to disk before eviction
            return pl.Flush()
        }),
    )

    if err != nil {
        return err
    }
    
    pl.lease = lease
    pl.bufferSize = size

    return nil
}

func (pl *PageLogger) Close() error {
    if pl.lease != nil {
        pl.manager.Release(pl.lease)
    }

    // ... existing cleanup

    return nil
}
```

### PageLogger Benefits

- Log buffers can be flushed under pressure
- Prevents memory exhaustion from buffered writes
- Graceful degradation via OnReclaim

## Full Stack Integration Example

```go
// Application setup
func NewApp(cfg *config.Config) *App {
    // Create memory manager
    memManager, _ := memory.NewManager(memory.Config{
        Capacity:  cfg.MemoryLimit,
        Threshold: 0.85,
    })
    
    // Create cluster with memory manager
    cluster := NewCluster(cfg, memManager)
    
    // Create database manager with memory manager
    dbManager := NewDatabaseManager(cluster, memManager)
    
    return &App{
        MemoryManager:   memManager,
        Cluster:         cluster,
        DatabaseManager: dbManager,
    }
}

// Cluster uses it for tiered FS
func (c *Cluster) TieredFS() *storage.FileSystem {
    if c.tieredFS == nil {
        c.tieredFS = storage.NewTieredFileSystemDriver(
            c.ctx,
            c.NetworkFS(),
            c.ObjectFS(),
            c.memManager, // Pass memory manager
        )
    }

    return c.tieredFS
}

// Database manager uses it for resources
func (dm *DatabaseManager) Resources(branch *Branch) *DatabaseResources {
    key := fmt.Sprintf("%s-%s", branch.DatabaseID, branch.DatabaseBranchID)
    
    if resources, exists := dm.resourcesCache.Get(key); exists {
        return resources.(*DatabaseResources)
    }
    
    resources := NewDatabaseResources(
        branch.DatabaseID,
        branch.DatabaseBranchID,
        dm.memManager, // Pass memory manager
    )
    
    dm.resourcesCache.Put(key, resources)

    return resources
}
```

## Monitoring Integration

Add metrics to observability stack:

```go
// Prometheus metrics example
func (app *App) RegisterMetrics(registry *prometheus.Registry) {
    memGauge := prometheus.NewGaugeFunc(
        prometheus.GaugeOpts{
            Name: "litebase_memory_reserved_bytes",
            Help: "Memory currently reserved",
        },
        func() float64 {
            return float64(app.MemoryManager.GetStats().Reserved)
        },
    )
    registry.MustRegister(memGauge)
    
    // Component-level metrics
    componentGauge := prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "litebase_memory_by_component_bytes",
            Help: "Memory usage by component",
        },
        []string{"component"},
    )
    
    // Update periodically
    go func() {
        ticker := time.NewTicker(10 * time.Second)

        for range ticker.C {
            metrics := app.MemoryManager.GetMetrics()
            for component, size := range metrics.GetMemoryByComponent() {
                componentGauge.WithLabelValues(component).Set(float64(size))
            }
        }
    }()
    
    registry.MustRegister(componentGauge)
}
```

## Testing Integrated Components

```go
func TestDatabaseWithMemoryManager(t *testing.T) {
    // Create memory manager with low limit for testing
    manager, _ := memory.NewManager(memory.Config{
        Capacity:  10 * 1024 * 1024, // 10MB
        Threshold: 0.9,
    })
    
    // Create database with memory-managed resources
    db := NewDatabase(manager)
    
    // Simulate memory pressure
    for i := 0; i < 1000; i++ {
        db.Put(fmt.Sprintf("key%d", i), make([]byte, 10*1024))
    }
    
    // Verify eviction occurred
    stats := manager.GetStats()
    metrics := manager.GetMetrics()
    
    if metrics.GetEvictionCount() == 0 {
        t.Error("Expected evictions under memory pressure")
    }
    
    if stats.Reserved > stats.Capacity {
        t.Error("Memory leak detected")
    }
}
```
