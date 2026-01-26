package memory

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"
)

// BufferPool manages a pool of fixed-size buffers with memory manager integration
type BufferPool struct {
	bufferSize int64
	manager    *Manager
	pool       sync.Pool
	leases     map[*[]byte]*Lease
	mutex      sync.Mutex
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(bufferSize int64, manager *Manager) *BufferPool {
	bp := &BufferPool{
		bufferSize: bufferSize,
		manager:    manager,
		leases:     make(map[*[]byte]*Lease),
	}

	bp.pool = sync.Pool{
		New: func() any {
			buf := make([]byte, bufferSize)
			return &buf
		},
	}

	return bp
}

// Get retrieves a buffer from the pool
func (bp *BufferPool) Get() (*[]byte, error) {
	// Request memory lease
	lease, err := bp.manager.Request(bp.bufferSize, WithOnReclaim(func() error {
		return nil
	}))

	if err != nil {
		return nil, err
	}

	// Get buffer from pool
	buf := bp.pool.Get().(*[]byte)

	// Track lease
	bp.mutex.Lock()
	bp.leases[buf] = lease
	bp.mutex.Unlock()

	return buf, nil
}

// Put returns a buffer to the pool
func (bp *BufferPool) Put(buf *[]byte) error {
	if buf == nil {
		return fmt.Errorf("cannot put nil buffer")
	}

	// Get and remove lease
	bp.mutex.Lock()
	lease, ok := bp.leases[buf]

	if ok {
		delete(bp.leases, buf)
	}

	bp.mutex.Unlock()

	// Release lease
	if ok && lease != nil {
		err := bp.manager.Release(lease)

		if err != nil {
			slog.Warn("Failed to release lease", "error", err)
		}
	}

	// Return buffer to pool
	bp.pool.Put(buf)

	return nil
}

// BytesBufferPool manages a pool of bytes.Buffer with memory manager integration
type BytesBufferPool struct {
	bufferSize int64
	manager    *Manager
	pool       sync.Pool
	leases     map[*bytes.Buffer]*Lease
	mutex      sync.Mutex
}

// NewBytesBufferPool creates a new bytes.Buffer pool
func NewBytesBufferPool(bufferSize int64, manager *Manager) *BytesBufferPool {
	bp := &BytesBufferPool{
		bufferSize: bufferSize,
		manager:    manager,
		leases:     make(map[*bytes.Buffer]*Lease),
	}

	bp.pool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, bufferSize))
		},
	}

	return bp
}

// Get retrieves a buffer from the pool
func (bp *BytesBufferPool) Get() (*bytes.Buffer, error) {
	// Request memory lease
	lease, err := bp.manager.Request(bp.bufferSize, WithOnReclaim(func() error {
		return nil
	}))

	if err != nil {
		return nil, err
	}

	// Get buffer from pool
	buf := bp.pool.Get().(*bytes.Buffer)
	buf.Reset()

	// Track lease
	bp.mutex.Lock()
	bp.leases[buf] = lease
	bp.mutex.Unlock()

	return buf, nil
}

// Put returns a buffer to the pool
func (bp *BytesBufferPool) Put(buf *bytes.Buffer) error {
	if buf == nil {
		return fmt.Errorf("cannot put nil buffer")
	}

	// Get and remove lease
	bp.mutex.Lock()
	lease, ok := bp.leases[buf]

	if ok {
		delete(bp.leases, buf)
	}

	bp.mutex.Unlock()

	// Release lease
	if ok && lease != nil {
		err := bp.manager.Release(lease)

		if err != nil {
			slog.Warn("Failed to release lease", "error", err)
		}
	}

	// Reset and return buffer to pool
	buf.Reset()
	bp.pool.Put(buf)

	return nil
}
