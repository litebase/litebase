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

## DatabaseManager/DatabaseResources Integration

### DatabaseManager Problem

DatabaseManager and Database use LFUCache with item-count limits, not memory
limits. Each cache operates independently with no global coordination:

- **DatabaseManager.databaseCache**: Caches Database instances (up to 1000)
- **DatabaseManager.keyCache**: Caches database name→ID mappings (up to 5000)
- **Database.branchCache**: Per-database branch cache (up to 100)

With millions of potential databases, these uncoordinated caches could consume
unbounded memory.

### DatabaseManager Solution

Replace all LFUCache instances with ManagedCache for memory-aware caching:

```go
// Cache configuration constants
const (
    // DatabaseCache holds Database instances
    DatabaseCacheCapacity    = 1000
    DatabaseCacheDefaultSize = 4096 // 4KB per database entry

    // KeyCache holds database name→ID mappings
    KeyCacheCapacity    = 5000
    KeyCacheDefaultSize = 128 // 128 bytes per key entry

    // BranchCache holds branch metadata per database
    BranchCacheCapacity    = 100
    BranchCacheDefaultSize = 2048 // 2KB per branch entry
)

type DatabaseManager struct {
    Cluster           *Cluster
    databaseCache     *memory.ManagedCache // Was: *cache.LFUCache
    keyCache          *memory.ManagedCache // Was: *cache.LFUCache
    // ... other fields
}

type Database struct {
    DatabaseManager *DatabaseManager
    branchCache     *memory.ManagedCache // Was: *cache.LFUCache
    // ... other fields
}

func NewDatabaseManager(cluster *Cluster) *DatabaseManager {
    dm := &DatabaseManager{
        Cluster: cluster,
        databaseCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    DatabaseCacheCapacity,
            Manager:     cluster.MemoryManager,
            DefaultSize: DatabaseCacheDefaultSize,
            Owner:       "database-cache",
        }),
        keyCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    KeyCacheCapacity,
            Manager:     cluster.MemoryManager,
            DefaultSize: KeyCacheDefaultSize,
            Owner:       "database-key-cache",
        }),
        // ... initialize other fields
    }

    return dm
}

func NewDatabase(databaseManager *DatabaseManager, name string) *Database {
    return &Database{
        DatabaseManager: databaseManager,
        Name:            name,
        branchCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    BranchCacheCapacity,
            Manager:     databaseManager.Cluster.MemoryManager,
            DefaultSize: BranchCacheDefaultSize,
            Owner:       fmt.Sprintf("branch-cache-%s", name),
        }),
        // ... other fields
    }
}
```

### Size Estimation

Cache entry sizes are defined as constants for consistency:

- **Database entries** (`DatabaseCacheDefaultSize`): 4KB - includes metadata,
  settings, timestamp fields
- **Key entries** (`KeyCacheDefaultSize`): 128 bytes - database name string + ID
- **Branch entries** (`BranchCacheDefaultSize`): 2KB - includes branch metadata,
  settings, parent references

These estimates ensure proper memory accounting even without precise measurement.
Using constants makes it easy to adjust globally if profiling reveals different
actual sizes.

### Cache Usage Patterns

```go
// DatabaseManager.Get - uses both caches
func (dm *DatabaseManager) Get(databaseID string) (*Database, error) {
    // Check database cache first
    if cached, exists := dm.databaseCache.Get(databaseID); exists {
        return cached.(*Database), nil
    }

    // Load from system database
    database, err := dm.loadDatabase(databaseID)

    if err != nil {
        return nil, err
    }

    // Cache the loaded database (respects memory limits)
    if err := dm.databaseCache.Put(databaseID, database); err != nil {
        log.Printf("Failed to cache database: %v", err)
    }

    return database, nil
}

// Database.Branch - uses branch cache
func (db *Database) Branch(name string) (*Branch, error) {
    // Load from system database to get branch ID
    branch, err := db.loadBranch(name)

    if err != nil {
        return nil, err
    }

    // Check cache using branch ID
    if cached, exists := db.branchCache.Get(branch.DatabaseBranchID); exists {
        return cached.(*Branch), nil
    }

    // Cache the branch (respects memory limits)
    if err := db.branchCache.Put(branch.DatabaseBranchID, branch); err != nil {
        log.Printf("Failed to cache branch: %v", err)
    }

    return branch, nil
}
```

### Memory Pressure Handling

Under memory pressure, ManagedCache automatically evicts LRU entries:

1. **Cache miss**: Entry evicted to free memory, reloaded from system database
2. **Cache put fails**: Entry not cached, but operation succeeds (degrades to
   pass-through)
3. **Multiple databases**: Memory distributed fairly via global limits

This ensures graceful degradation rather than hard failures.

### Benefits

- **Global memory limits**: All caches respect shared memory budget
- **Fair allocation**: Memory distributed across databases automatically
- **Graceful degradation**: Cache misses handled transparently via database reload
- **Visibility**: Track memory usage per cache component
- **Coordinated eviction**: LRU policy applies across all caches globally

## PageLogger/PageLog Integration

### PageLog Problem

PageLog uses LFUCache with fixed 100-entry limit for page caching. Each page is
4KB (SQLite page size), so a single PageLog can cache up to 400KB without any
global coordination. With thousands of PageLogs across many databases, total
memory usage is unbounded.

### PageLog Solution

Replace LFUCache with ManagedCache for memory-aware page caching:

```go
// Page log cache configuration constants
const (
    PageSize = 4096

    // PageLogCache holds page data in memory
    PageLogCacheCapacity    = 100
    PageLogCacheDefaultSize = 4096 // 4KB per page (SQLite page size)
)

type PageLog struct {
    cache         *memory.ManagedCache // Was: *cache.LFUCache
    memoryManager *memory.Manager
    // ... other fields
}

func NewPageLog(
    fileSystem *FileSystem,
    path string,
    memoryManager *memory.Manager,
) (*PageLog, error) {
    pl := &PageLog{
        cache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    PageLogCacheCapacity,
            Manager:     memoryManager,
            DefaultSize: PageLogCacheDefaultSize,
            Owner:       fmt.Sprintf("page-log-cache-%s", path),
        }),
        encrypted:     false,
        fileSystem:    fileSystem,
        memoryManager: memoryManager,
        mutex:         &sync.Mutex{},
        Path:          path,
    }

    // ... rest of initialization

    return pl, nil
}
```

### Memory Manager Flow

Memory manager flows through the initialization chain:

```text
Cluster.MemoryManager
    ↓
DatabaseManager (passes to PageLogManager)
    ↓
PageLogManager (creates PageLoggers)
    ↓
PageLogger (creates PageLogs)
    ↓
PageLog.cache (ManagedCache)
```

### PageLog Implementation Details

#### Constant Configuration

Size estimates are defined as constants for consistency:

- **PageLogCacheCapacity**: 100 pages per log
- **PageLogCacheDefaultSize**: 4KB (matches SQLite page size)

#### Memory Coordination

All PageLog caches across all databases share the global memory budget:

- Each PageLog can cache up to 400KB (100 pages × 4KB)
- Under memory pressure, LRU pages are evicted across all PageLogs globally
- Page cache misses result in reading from disk (transparent to callers)

### PageLog Benefits

- **Global memory limits**: All page caches respect shared memory budget
- **Fair allocation**: Memory distributed across all databases automatically
- **Graceful degradation**: Cache misses handled by disk reads
- **Visibility**: Track page cache memory usage per PageLog
- **Prevents OOM**: Page caching bounded by global memory limit

## Full Stack Integration Example

```go
// Cluster initialization with memory manager
func NewCluster(cfg *config.Config) (*Cluster, error) {
    cluster := &Cluster{
        Config: cfg,
        // ... other fields
    }

    // Create memory manager with configured limit
    memoryLimit := cfg.MemoryLimit
    if memoryLimit == 0 {
        memoryLimit = 1 * 1024 * 1024 * 1024 // Default: 1GB
    }

    cluster.MemoryManager = memory.NewManager(memory.ManagerConfig{
        Capacity:  memoryLimit,
        Threshold: 0.85,
    })

    return cluster, nil
}

// TieredFS uses memory manager for buffers
func (c *Cluster) TieredFS() *storage.FileSystem {
    if c.tieredFileSystem == nil {
        c.fileSystemMutex.Lock()
        defer c.fileSystemMutex.Unlock()

        if c.tieredFileSystem != nil {
            return c.tieredFileSystem
        }

        c.tieredFileSystem = storage.NewFileSystem(
            storage.NewTieredFileSystemDriver(
                c.Node().Context(),
                c.NetworkFS(),
                c.ObjectFS(),
                c.MemoryManager, // Pass memory manager for buffer pool
                fileSyncEligibilityFn,
            ),
        )
    }

    return c.tieredFileSystem
}

// DatabaseManager initialization
func NewDatabaseManager(
    cluster *Cluster,
    secretsManager *auth.SecretsManager,
) *DatabaseManager {
    dm := &DatabaseManager{
        Cluster:           cluster,
        databaseCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    1000,
            Manager:     cluster.MemoryManager,
            DefaultSize: 4096,
            Owner:       "database-cache",
        }),
        keyCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    5000,
            Manager:     cluster.MemoryManager,
            DefaultSize: 128,
            Owner:       "database-key-cache",
        }),
        // ... other fields
    }

    return dm
}

// ConnectionManager gets memory manager from cluster
func (dm *DatabaseManager) ConnectionManager() *ConnectionManager {
    dm.connectionManagerMutex.Lock()
    defer dm.connectionManagerMutex.Unlock()

    if dm.connectionManager != nil {
        return dm.connectionManager
    }

    dm.connectionManager = &ConnectionManager{
        cluster:         dm.Cluster,
        connectionSize:  256 * 1024, // 256KB per connection
        databaseManager: dm,
        databases:       map[string]*DatabaseGroup{},
        memoryManager:   dm.Cluster.MemoryManager, // Get from cluster
        mutex:           &sync.RWMutex{},
        state:           ConnectionManagerStateRunning,
    }

    // Start connection monitoring ticker
    go dm.connectionManager.monitorConnections()

    return dm.connectionManager
}

// Database gets memory manager through DatabaseManager
func NewDatabase(databaseManager *DatabaseManager, name string) *Database {
    return &Database{
        DatabaseManager: databaseManager,
        Name:            name,
        branchCache: memory.NewManagedCache(memory.ManagedCacheConfig{
            Capacity:    100,
            Manager:     databaseManager.Cluster.MemoryManager,
            DefaultSize: 2048,
            Owner:       fmt.Sprintf("branch-cache-%s", name),
        }),
        // ... other fields
    }
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
