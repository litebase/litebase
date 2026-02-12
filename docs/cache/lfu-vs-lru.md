# Cache Implementation Guide

## Overview

Litebase now supports both **LFU (Least Frequently Used)** and **LRU (Least Recently Used)** caching strategies with memory management integration.

## Available Cache Types

### 1. ManagedLFUCache (Default)

- **Strategy**: Evicts least frequently used items first
- **Best for**: Workloads with repeated access to the same data
- **Default capacity**: 32,000 pages for WAL cache

### 2. ManagedLRUCache (New)

- **Strategy**: Evicts least recently used items first  
- **Best for**: Sequential or time-based access patterns
- **Recommended for**: Large working sets with temporal locality

## Usage Examples

### Using LFU Cache (Current Default)

```go
import "github.com/litebase/litebase/pkg/memory"

cache := memory.NewManagedLFUCache(memory.ManagedLFUCacheConfig{
    Capacity:    32000,
    Manager:     memoryManager,
    DefaultSize: 4096,
    Owner:       "my-cache",
})
```

### Using LRU Cache (For Testing)

```go
import "github.com/litebase/litebase/pkg/memory"

cache := memory.NewManagedLRUCache(memory.ManagedLRUCacheConfig{
    Capacity:    32000,
    Manager:     memoryManager,
    DefaultSize: 4096,
    Owner:       "my-cache",
})
```

### Backward Compatibility

The old `ManagedCache` type is aliased to `ManagedLFUCache` for backward compatibility:

```go
// These are equivalent:
cache1 := memory.NewManagedCache(config)
cache2 := memory.NewManagedLFUCache(config)
```

## Testing LRU vs LFU for WAL Cache

To test LRU cache for the WAL, modify `pkg/database/database_wal.go`:

```go
// Change from:
cache: memory.NewManagedCache(memory.ManagedCacheConfig{
    Capacity:    WALCacheCapacity,
    Manager:     memoryManager,
    DefaultSize: WALCacheDefaultSize,
    Owner:       fmt.Sprintf("wal-cache-%s-%s-%d", databaseId, branchId, timestamp),
}),

// To:
cache: memory.NewManagedLRUCache(memory.ManagedLRUCacheConfig{
    Capacity:    WALCacheCapacity,
    Manager:     memoryManager,
    DefaultSize: WALCacheDefaultSize,
    Owner:       fmt.Sprintf("wal-cache-%s-%s-%d", databaseId, branchId, timestamp),
}),
```

## Test Results

From the vector search test with 250,000 vectors:

- **Unique pages accessed**: 73,749
- **WAL cache capacity**: 32,000 pages (increased from 10,000)
- **Working set**: 2.3x cache capacity

### Read Statistics (LFU)

- Transaction buffer reads: 32.7%
- Cache hits: 7.0%
- File reads (cache miss): 60.3%

The high cache miss rate suggests that an LRU strategy might perform better for this workload, as it has strong temporal locality (recent writes are likely to be read soon).

## Recommendations

1. **For vector operations**: Try LRU cache - these workloads often have sequential/temporal access patterns
2. **For general WAL**: Stick with LFU cache - balanced performance across different workloads
3. **Monitor cache hit rates**: Use debug logging to evaluate effectiveness

## Implementation Details

### LFU Cache

- Uses min-heap priority queue
- Tracks access frequency per item
- O(log n) for eviction decisions

### LRU Cache  

- Uses doubly-linked list
- Tracks recency of access
- O(1) for eviction decisions

Both implementations:

- Use buffer pools to reduce allocations
- Support byte slice copying for safety
- Integrate with memory manager for pressure-based eviction
- Thread-safe with mutex protection
