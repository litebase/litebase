# Memory Manager

The Memory Manager provides centralized memory coordination for Litebase, preventing out-of-memory situations when managing many databases.

## Documentation

- [Overview](overview.md) - Architecture and how the memory manager works
- [Usage Guide](usage.md) - API reference and examples
- [Integration Guide](integration.md) - How to integrate with Litebase components

## Quick Start

```go
// Create a memory manager
manager, _ := memory.NewManager(memory.Config{
    Capacity:  100 * 1024 * 1024, // 100MB
    Threshold: 0.9,
})

// Request memory
lease, err := manager.Request(4096)

if err != nil {
    return err
}

defer manager.Release(lease)

// Use memory-managed buffer pool
pool := memory.NewBufferPool(4096, manager)
buf, _ := pool.Get()

defer pool.Put(buf)

// Use memory-managed cache
cache := memory.NewManagedCache(memory.ManagedCacheConfig{
    Capacity:    1000,
    Manager:     manager,
    DefaultSize: 64,
})

cache.Put("key", "value")
```

## Performance

- **2.6M allocations/sec** - High throughput
- **458 ns/op** - Sub-microsecond latency
- **120 bytes/op** - Minimal overhead
- **Thread-safe** - Lock-free reservoir operations

## Testing

```bash
# Run all tests
go test ./pkg/memory/...

# Run with race detector
go test -race ./pkg/memory/...

# Run benchmarks
go test -bench=. ./pkg/memory/...
```

## Components

### Core

- **Manager** ([manager.go](../../pkg/memory/manager.go)) - Central coordinator
- **Reservoir** ([reservoir.go](../../pkg/memory/reservoir.go)) - Memory pool
- **Eviction** ([eviction.go](../../pkg/memory/eviction.go)) - LRU and size-based policies
- **Metrics** ([metrics.go](../../pkg/memory/metrics.go)) - Usage tracking

### Adapters

- **BufferPool** ([buffer_pool.go](../../pkg/memory/buffer_pool.go)) - sync.Pool replacement
- **ManagedCache** ([cache_adapter.go](../../pkg/memory/cache_adapter.go)) - LFU cache wrapper
