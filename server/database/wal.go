package database

import (
	"io"
	"sync"
)

type WAL struct {
	mutex *sync.Mutex
	pages map[int64][]byte
}

func NewWAL() *WAL {
	return &WAL{
		mutex: &sync.Mutex{},
		pages: map[int64][]byte{},
	}
}

func (wal *WAL) Close() {
	wal.pages = nil
}

func (wal *WAL) Has(pageNumber int64) bool {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	_, ok := wal.pages[pageNumber]
	return ok
}

func (wal *WAL) Read(pageNumber int64, pageOffset int64, data []byte) (n int, err error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if _, ok := wal.pages[pageNumber]; !ok {
		return 0, io.EOF
	}

	n = copy(data, wal.pages[pageNumber][pageOffset:])

	return n, err
}

func (wal *WAL) Write(pageNumber int64, data []byte) (int, error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if _, ok := wal.pages[pageNumber]; !ok {
		wal.pages[pageNumber] = make([]byte, 4096)
	}

	n := copy(wal.pages[pageNumber], data)

	return n, nil
}
