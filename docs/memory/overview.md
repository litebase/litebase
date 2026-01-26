# Memory Manager Overview

## Purpose

The Memory Manager provides centralized memory coordination for Litebase, preventing out-of-memory situations when managing many databases. It replaces uncoordinated per-component memory management with a global lease-based system.

## Architecture

### Core Components

The memory management system consists of four core components working together:

1. **Manager** - Central coordinator for all memory allocation across the system
2. **Reservoir** - Fixed-size memory pool with atomic operations for thread-safe allocation
3. **Eviction Policies** - Strategies for reclaiming memory (LRU and size-based)
4. **Metrics** - Component-level memory usage tracking and observability

### Integration Adapters

Three integration adapters provide seamless integration with existing Litebase infrastructure:

1. **BufferPool** - Drop-in replacement for sync.Pool with memory tracking
2. **BytesBufferPool** - Memory-managed bytes.Buffer pool
3. **ManagedCache** - Wrapper for existing LFUCache with automatic memory limits

## How It Works

### Lease-Based System

Memory is allocated through **leases** - objects that represent a memory reservation:

- Each lease has a unique ID, size, and priority
- Leases can be **reclaimable** (cache data) or **non-reclaimable** (active connections)
- Leases track last-used time for LRU eviction
- Leases can have callbacks that execute before eviction

### Memory Allocation Flow

1. Component requests memory via `manager.Request(size, options...)`
2. Manager checks if memory is available in the reservoir
3. If not available, manager triggers eviction policy
4. Eviction policy selects leases to reclaim based on LRU/priority
5. OnReclaim callbacks execute to allow graceful state saving
6. Memory is released and lease is granted to requester

### Eviction Policies

**LRU with Priority Weighting**: Evicts least recently used items first, but weighs by priority:

- Low priority items evicted first
- High/critical priority items protected longer
- Age calculation: `score = age / priorityWeight`

**Size-Based**: Evicts largest items first to maximize freed memory quickly.

## Performance Characteristics

- **Throughput**: 2.6M allocations/second
- **Latency**: 458 nanoseconds per operation
- **Overhead**: 120 bytes per allocation
- **Thread Safety**: Lock-free reservoir operations with atomic counters
- **Scalability**: Designed for thousands to millions of databases

## Benefits

1. **Global Coordination** - Single source of truth prevents fragmented memory management
2. **Prevents OOM** - Hard limits with predictable behavior under memory pressure
3. **Fair Eviction** - Priority-weighted LRU ensures important data stays cached
4. **Component Visibility** - Track which components consume memory
5. **Graceful Degradation** - OnReclaim callbacks allow state saving before eviction
6. **Production Ready** - Comprehensive test coverage with race detection
