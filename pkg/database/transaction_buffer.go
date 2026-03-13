package database

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"github.com/litebase/litebase/pkg/memory"
)

var (
	ErrNotBuffered       = errors.New("data not in buffer")
	ErrBufferCapacity    = errors.New("buffer capacity exceeded")
	DefaultBufferSize    = int64(16 * 1024 * 1024) // 16MB default
	MinimumBufferSize    = int64(8 * 1024 * 1024)  // 8MB minimum
	PreferredBufferSizes = []int64{
		64 * 1024 * 1024, // 64MB preferred
		32 * 1024 * 1024, // 32MB fallback
		16 * 1024 * 1024, // 16MB fallback
		8 * 1024 * 1024,  // 8MB minimum
	}

	// SQLite WAL format constants (https://sqlite.org/walformat.html)
	// WAL Header (32 bytes) - must be written immediately for WAL integrity
	WALHeaderSize = int64(32)
)

// WALWriter interface for writing to WAL (implemented by both Database WAL and DatabaseWALManager)
type WALWriter interface {
	WriteAt(p []byte, off int64) (n int, err error)
}

// DirectWALWriter can write directly to WAL bypassing buffer (used for buffer flush)
type DirectWALWriter interface {
	WriteDirectToLatestWAL(p []byte, off int64) (n int, err error)
}

// walWrite represents a single buffered write to the WAL file.
// SQLite WAL is append-only, so we preserve write order.
type walWrite struct {
	offset int64
	data   []byte
}

// TransactionBuffer provides a transaction-scoped write buffer for WAL operations.
// It respects SQLite's append-only WAL format by preserving write order.
// WAL Header (first 32 bytes) is written immediately to maintain integrity.
// All subsequent frame writes are buffered and flushed in FIFO order.
// Memory is allocated from a manager-owned slab to avoid per-write allocations.
type TransactionBuffer struct {
	writes      []walWrite    // Ordered slice preserving append-only WAL format
	coalesced   []walWrite    // Reusable scratch for GetCoalescedWrites (avoids per-flush alloc)
	offsetIndex map[int64]int // Quick lookup: offset -> index in writes slice
	mutex       sync.RWMutex
	memoryLease *memory.Lease
	slab        []byte // Manager-owned memory slab
	slabOffset  int64  // Current offset in slab (bump allocator)
	capacity    int64
	used        int64
}

// NewTransactionBuffer creates a new transaction buffer with dynamic sizing.
// It attempts to allocate the largest buffer size available from memory manager,
// falling back to smaller sizes on allocation failure.
// Memory is allocated from a manager-owned slab to avoid per-write heap allocations.
func NewTransactionBuffer(
	memoryManager *memory.Manager, walTimestamp int64,
) (*TransactionBuffer, error) {
	if memoryManager == nil {
		return nil, errors.New("memory manager is required")
	}

	var lease *memory.Lease
	var err error
	var allocatedSize int64

	// Try preferred sizes in descending order
	for _, size := range PreferredBufferSizes {
		lease, err = memoryManager.RequestWithSlab(
			size,
			memory.Reclaimable(false),
			memory.WithPriority(memory.PriorityHigh),
			memory.WithOwner(fmt.Sprintf("tx-buffer-%d", walTimestamp)),
		)

		if err == nil {
			allocatedSize = size
			break
		}

		slog.Debug("Failed to allocate transaction buffer",
			"size", size,
			"error", err)
	}

	if lease == nil {
		return nil, fmt.Errorf("failed to allocate transaction buffer: %w", err)
	}

	buf := &TransactionBuffer{
		writes:      make([]walWrite, 0, 1024), // Pre-allocate for ~1024 frames
		coalesced:   make([]walWrite, 0, 1024), // Reuse across flushes
		offsetIndex: make(map[int64]int, 1024), // Match writes pre-allocation
		memoryLease: lease,
		slab:        lease.Slab,
		slabOffset:  0,
		capacity:    allocatedSize,
	}

	return buf, nil
}

// WriteAt buffers a write at the given offset using slab-backed storage.
// SQLite WAL is append-only, so we preserve write order in a slice.
// Auto-returns error when capacity reached (caller should flush).
// Memory is suballocated from the manager-owned slab (no per-write allocations).
func (b *TransactionBuffer) WriteAt(p []byte, off int64) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	writeSize := int64(len(p))

	// Check capacity
	if b.used+writeSize > b.capacity {
		return 0, ErrBufferCapacity
	}

	// Suballocate from slab (bump allocator)
	if b.slabOffset+writeSize > int64(len(b.slab)) {
		return 0, ErrBufferCapacity
	}

	// Copy data into slab slice (no heap allocation)
	data := b.slab[b.slabOffset : b.slabOffset+writeSize]
	copy(data, p)
	b.slabOffset += writeSize

	// Check if we're overwriting an existing offset (uncommon but possible)
	if idx, exists := b.offsetIndex[off]; exists {
		// Update existing write in-place
		oldSize := int64(len(b.writes[idx].data))
		b.writes[idx].data = data
		b.used += writeSize - oldSize
	} else {
		// Append new write (preserves order for append-only WAL)
		b.writes = append(b.writes, walWrite{offset: off, data: data})
		b.offsetIndex[off] = len(b.writes) - 1
		b.used += writeSize
	}

	return len(p), nil
}

// ReadAt reads data from the buffer at the given offset.
// Returns ErrNotBuffered if the data is not in the buffer.
func (b *TransactionBuffer) ReadAt(p []byte, off int64) (int, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// Look up write by offset
	idx, exists := b.offsetIndex[off]

	if !exists {
		return 0, ErrNotBuffered
	}

	data := b.writes[idx].data

	if len(data) < len(p) {
		return 0, ErrNotBuffered
	}

	n := copy(p, data)

	return n, nil
}

// Contains checks if the buffer contains data at the given offset and length.
func (b *TransactionBuffer) Contains(offset int64, length int) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	idx, exists := b.offsetIndex[offset]

	if !exists {
		return false
	}

	return len(b.writes[idx].data) >= length
}

// GetWrites returns all buffered writes for flushing.
// The returned slice is the live backing slice — valid as long as the caller
// holds the SQLite write lock (which prevents concurrent WriteAt calls).
// Caller must not modify the returned slice.
func (b *TransactionBuffer) GetWrites() []walWrite {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.writes
}

// GetCoalescedWrites returns writes merged into the fewest possible contiguous
// spans, for use in the flush path to reduce file.WriteAt call count.
//
// Two adjacent entries can be merged when:
//  1. Their WAL offsets are contiguous: writes[j].offset == writes[j-1].offset + len(writes[j-1].data)
//  2. Their data slices are adjacent in the slab (no gap, no overwrite hole).
//
// Merging is zero-allocation: it re-slices the already-slab-backed data slice
// (the slab capacity covers all later entries in the same bump region).
// The returned slice reuses b.coalesced — valid until the next call to
// GetCoalescedWrites or Clear.
func (b *TransactionBuffer) GetCoalescedWrites() []walWrite {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.writes) == 0 {
		return nil
	}

	b.coalesced = b.coalesced[:0]
	cur := b.writes[0]

	for i := 1; i < len(b.writes); i++ {
		next := b.writes[i]

		// Condition 1: sequential WAL offsets.
		sequential := next.offset == cur.offset+int64(len(cur.data))

		// Condition 2: data slices are adjacent in the slab (no allocation holes
		// from overwrites). We compare raw pointers to avoid any allocation.
		var slabAdjacent bool

		if sequential && len(cur.data) > 0 && len(next.data) > 0 {
			curEnd := unsafe.Add(unsafe.Pointer(unsafe.SliceData(cur.data)), len(cur.data))
			nextStart := unsafe.Pointer(unsafe.SliceData(next.data))
			slabAdjacent = curEnd == nextStart
		}

		if sequential && slabAdjacent {
			// Extend cur.data to span next — zero allocation: the slab backing
			// provides the capacity because next.data occupies the immediately
			// following bytes in the same slab allocation.
			cur.data = cur.data[:len(cur.data)+len(next.data)]
		} else {
			b.coalesced = append(b.coalesced, cur)
			cur = next
		}
	}

	b.coalesced = append(b.coalesced, cur)

	return b.coalesced
}

// Clear resets the buffer after successful flush.
// Preserves the slab for reuse (bump allocator resets to start).
func (b *TransactionBuffer) Clear() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.writes = b.writes[:0]        // Reuse slice capacity
	for k := range b.offsetIndex { // Clear in-place: reuses bucket array
		delete(b.offsetIndex, k)
	}

	b.used = 0
	b.slabOffset = 0 // Reset bump allocator
}

// Discard clears the buffer without flushing to WAL.
// This is called on transaction rollback.
func (b *TransactionBuffer) Discard() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	slog.Debug("Discarding transaction buffer",
		"writes", len(b.writes),
		"used", b.used)

	b.writes = b.writes[:0]
	for k := range b.offsetIndex { // Clear in-place: reuses bucket array
		delete(b.offsetIndex, k)
	}

	b.used = 0
	b.slabOffset = 0
}

// Release frees the memory lease and slab.
// This is idempotent and safe to call multiple times.
func (b *TransactionBuffer) Release(memoryManager *memory.Manager) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.memoryLease == nil {
		return nil // Already released
	}

	err := memoryManager.Release(b.memoryLease)
	b.memoryLease = nil
	b.slab = nil // Release slab reference for GC

	return err
}

// Stats returns buffer statistics for debugging.
func (b *TransactionBuffer) Stats() (writes int, used int64, capacity int64) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.writes), b.used, b.capacity
}
