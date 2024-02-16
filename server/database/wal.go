package database

import (
	"io"
	"litebasedb/internal/config"
	"sync"
)

type WAL struct {
	mutex *sync.RWMutex
	pages map[int64][]byte
}

func NewWAL() *WAL {
	return &WAL{
		mutex: &sync.RWMutex{},
		pages: map[int64][]byte{},
	}
}

func (wal *WAL) Close() {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.pages = nil
}

func (wal *WAL) Has(pageNumber int64) bool {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()

	_, ok := wal.pages[pageNumber]
	return ok
}

func (wal *WAL) Read(pageNumber int64, pageOffset int64) (data []byte, err error) {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()

	if _, ok := wal.pages[pageNumber]; !ok {
		return nil, io.EOF
	}

	return wal.pages[pageNumber][pageOffset:], err
}

func (wal *WAL) Write(pageNumber int64, data []byte) (int, error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if _, ok := wal.pages[pageNumber]; !ok {
		wal.pages[pageNumber] = make([]byte, config.Get().PageSize)
	}

	n := copy(wal.pages[pageNumber], data)

	return n, nil
}
