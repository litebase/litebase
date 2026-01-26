# Integration Guide

## Overview

This guide shows how to integrate the Memory Manager with existing Litebase components.

## TieredFileSystemDriver Integration

### Problem

The TieredFileSystemDriver currently uses raw `sync.Pool` for buffer management with no memory tracking. With potentially 10,000+ open files per database, memory usage can spiral out of control.

### Solution

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
        bufferPool: memory.NewBufferPool(32*1024, manager), // 32KB buffers
        manager:    manager,
        // ... initialize other fields
    }
}

func (fsd *TieredFileSystemDriver) CopyFile(dst io.Writer, src io.Reader) (int64, error) {
    buf, err := fsd.bufferPool.Get()

    if err != nil {
        return 0, fmt.Errorf("failed to get buffer: %w", err)
    }

    defer fsd.bufferPool.Put(buf)
    
    return io.CopyBuffer(dst, src, *buf)
}
```

### Benefits

- Buffer memory is tracked globally
- Under pressure, buffers can be evicted
- No more unbounded buffer pool growth

## ConnectionManager Integration

### Problem

The ConnectionManager has unbounded connection maps with only idle timeout. No global memory limits exist.

### Solution

Request memory leases for each connection:

```go
type ConnectionManager struct {
    manager        *memory.Manager
    connectionSize int64 // Estimated size per connection
    // ... existing fields
}

func (cm *ConnectionManager) Get(dbID, branchID string) (*Connection, error) {
    // Request memory for connection
    lease, err := cm.manager.Request(cm.connectionSize,
        memory.Reclaimable(false), // Connections can't be evicted
        memory.WithPriority(memory.PriorityHigh),
        memory.WithOwner("connection-pool"),
    )

    if err != nil {
        return nil, fmt.Errorf("insufficient memory for connection: %w", err)
    }
    
    conn, err := cm.getOrCreateConnection(dbID, branchID)
    
    if err != nil {
        cm.manager.Release(lease)
        return nil, err
    }
    
    conn.lease = lease
    
    return conn, nil
}

func (cm *ConnectionManager) Release(conn *Connection) {
    if conn.lease != nil {
        cm.manager.Release(conn.lease)
    }
    // ... existing cleanup
}
```

### Benefits

- Hard limit on total connections across all databases
- Fair allocation across databases
- Prevents OOM from too many connections

## DatabaseResources Integration

### Problem

DatabaseResources uses LFUCache with item-count limits, not memory limits. Each cache is independent.

### Solution

Use ManagedCache for all caches:

```go
type DatabaseResources struct {
    manager      *memory.Manager
    databaseCache *memory.ManagedCache
    keyCache     *memory.ManagedCache
    // ... other resources
}

func NewDatabaseResources(dbID, branchID string, manager *memory.Manager) *DatabaseResources {
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

### Benefits

- Memory limits instead of arbitrary item counts
- Caches compete fairly for memory
- Component-level visibility

## PageLogger/SnapshotLogger Integration

### Problem

Page loggers buffer writes in memory with no global coordination.

### Solution

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

### Benefits

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
