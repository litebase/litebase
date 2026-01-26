# Memory Manager Usage

## Basic Allocation

### Creating a Manager

```go
manager, err := memory.NewManager(memory.Config{
    Capacity:  1024 * 1024 * 100, // 100MB total memory budget
    Threshold: 0.9,               // Trigger eviction at 90% utilization
})

if err != nil {
    return err
}
```

### Requesting Memory

```go
// Basic request (reclaimable, normal priority)
lease, err := manager.Request(4096)

if err != nil {
    return err
}

defer manager.Release(lease)

// Use the allocated memory...
```

### Lease Options

```go
lease, err := manager.Request(4096,
    memory.Reclaimable(true),                  // Can be evicted under pressure
    memory.WithPriority(memory.PriorityHigh),  // Evict last
    memory.WithOwner("connection-pool"),       // Track by component
    memory.WithOnReclaim(func() error {        // Callback before eviction
        // Save state, close connections, flush buffers, etc.
        return nil
    }),
)
```

## Buffer Pools

### Fixed-Size Buffers

```go
// Create pool for 4KB buffers
pool := memory.NewBufferPool(4096, manager)

// Get a buffer
buf, err := pool.Get()

if err != nil {
    return err
}

// Use the buffer
copy(*buf, data)

// Return to pool
pool.Put(buf)
```

### Bytes.Buffer Pool

```go
// Create pool for 1KB buffers
pool := memory.NewBytesBufferPool(1024, manager)

// Get a buffer
buf, err := pool.Get()

if err != nil {
    return err
}

// Use the buffer
buf.WriteString("Hello, World!")

result := buf.String()

// Return to pool (automatically resets)
pool.Put(buf)
```

## Managed Cache

### Basic Cache Operations

```go
cache := memory.NewManagedCache(memory.ManagedCacheConfig{
    Capacity:    1000,              // Max 1000 items
    Manager:     manager,           // Memory manager instance
    DefaultSize: 64,                // Default size per item
    Owner:       "page-cache",      // Component name for metrics
})

// Add items
err := cache.Put("key1", "value1")

if err != nil {
    return err
}

// Retrieve items (touches lease for LRU)
value, found := cache.Get("key1")

if found {
    // Use value...
}

// Delete items
cache.Delete("key1")
```

### Custom Size Calculation

```go
cache := memory.NewManagedCache(memory.ManagedCacheConfig{
    Capacity: 1000,
    Manager:  manager,
    SizeFunc: func(value any) int64 {
        switch v := value.(type) {
        case []byte:
            return int64(len(v))
        case string:
            return int64(len(v))
        case *MyStruct:
            return v.EstimateSize()
        default:
            return 64 // Default fallback
        }
    },
    Owner: "custom-cache",
})

// Now byte slices use actual size
cache.Put("data", make([]byte, 4096)) // Uses 4096 bytes, not 64
```

## Monitoring

### Getting Statistics

```go
// Get current memory statistics
stats := manager.GetStats()

fmt.Printf("Capacity: %d bytes\n", stats.Capacity)
fmt.Printf("Reserved: %d bytes\n", stats.Reserved)
fmt.Printf("Available: %d bytes\n", stats.Available)
fmt.Printf("Utilization: %.2f%%\n", stats.UtilizationPercent)
fmt.Printf("Under Pressure: %v\n", stats.UnderPressure)
```

### Component-Level Metrics

```go
// Get detailed metrics
metrics := manager.GetMetrics()

// Total operations
fmt.Printf("Total Allocated: %d bytes\n", metrics.GetTotalReserved())
fmt.Printf("Total Released: %d bytes\n", metrics.GetTotalReleased())
fmt.Printf("Evictions: %d\n", metrics.GetEvictionCount())
fmt.Printf("Failed Reclaims: %d\n", metrics.GetReclaimFailures())

// Memory by component
byComponent := metrics.GetMemoryByComponent()

for component, size := range byComponent {
    fmt.Printf("  %s: %d bytes\n", component, size)
}
```

### Checking Allocation Possibility

```go
// Check if allocation is possible without triggering eviction
if manager.CanAllocate(requiredSize) {
    // Safe to allocate
    lease, _ := manager.Request(requiredSize)

    defer manager.Release(lease)
} else {
    // Would trigger eviction or fail
}
```

## Lifecycle Management

### Touching Leases

Update the last-used time to prevent eviction:

```go
lease, _ := manager.Request(4096)

// Later, when accessing the resource
manager.Touch(lease)

// Release when done
defer manager.Release(lease)
```

### Graceful Shutdown

```go
// Release all leases and shutdown
err := manager.Shutdown()

if err != nil {
    return err
}
```

This calls all OnReclaim callbacks and releases all memory.

## Error Handling

```go
lease, err := manager.Request(size)

if err == memory.ErrNoMemory {
    // Insufficient memory even after eviction
    return fmt.Errorf("out of memory")
} else if err == memory.ErrManagerShutdown {
    // Manager has been shutdown
    return fmt.Errorf("manager shutdown")
} else if err != nil {
    // Other error
    return err
}
```
