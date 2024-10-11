package storage

import (
	"errors"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	ErrWalOutOfSync        = errors.New("wal out of sync with primary")
	ErrWalSequenceMismatch = errors.New("wal sequence mismatch")
)

type WalFile struct {
	file       internalStorage.File
	mutex      *sync.Mutex
	sequence   int64
	timestamp  int64
	writeQueue []WalWrite
}

type WalWrite struct {
	Data      []byte
	Offset    int64
	Sequence  int64
	Timestamp int64
}

func NewWalFile(path string) (*WalFile, error) {
	fs := TmpFS()

tryOpen:
	file, err := fs.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)

	if os.IsNotExist(err) {
		if err := fs.MkdirAll(filepath.Base(path), 0755); err != nil {
			return nil, err
		}

		goto tryOpen

	} else {
		if err != nil {
			return nil, err
		}
	}

	return &WalFile{
		file:       file,
		mutex:      &sync.Mutex{},
		writeQueue: []WalWrite{},
	}, nil
}

/*
Apply items that were queued to be written to the WAL. These items should be
only be written if the sequence number is in order.
*/
func (w *WalFile) applyWriteQueue() {
	sequenceNumbers := []int64{}

	if len(w.writeQueue) == 0 {
		return
	}

	for _, write := range w.writeQueue {
		sequenceNumbers = append(sequenceNumbers, write.Sequence)
	}

	sort.Slice(sequenceNumbers, func(i, j int) bool {
		return sequenceNumbers[i] < sequenceNumbers[j]
	})

	for {
		applied := false
		var sequenceNumersToRemove []int64

		for _, seq := range sequenceNumbers {
			if seq == w.sequence+1 {
				sequenceNumersToRemove = append(sequenceNumersToRemove, seq)

				for _, write := range w.writeQueue {
					if write.Sequence == seq {
						_, err := w.file.WriteAt(write.Data, write.Offset)

						if err != nil {
							log.Println("Failed to write to wal file", err)
							break
						}

						applied = true
						break
					}
				}
			}
		}

		// Remove applied sequence numbers from the queue
		for _, seq := range sequenceNumersToRemove {
			for i := len(sequenceNumbers) - 1; i >= 0; i-- {
				if sequenceNumbers[i] == seq {
					sequenceNumbers = append(sequenceNumbers[:i], sequenceNumbers[i+1:]...)
				}
			}
		}

		if !applied {
			break
		}
	}
}

/*
Close the WAL file.
*/
func (w *WalFile) Close() error {
	return w.file.Close()
}

/*
Return the current timestamp of the WAL file.
*/
func (w *WalFile) Timestamp() int64 {
	return w.timestamp
}

/*
Truncate the WAL file to the given size.
*/
func (w *WalFile) Truncate(size, sequence, timestamp int64) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Apply items that were queued to be written
	w.applyWriteQueue()

	w.sequence = sequence
	w.timestamp = timestamp
	w.writeQueue = []WalWrite{}

	// TODO: Signal to database connections to use SQLITE_FCNTL_RESET_CACHE?

	return w.file.Truncate(size)
}

/*
Write to the WAL file at the given offset.
*/
func (w *WalFile) WriteAt(p []byte, off, sequence, timestamp int64) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Apply items that were queued to be written
	w.applyWriteQueue()

	// Handle write attempts to the WAL that are not the first sequence and the
	// local WAL sequence is not 0, meaning the WAL files are not in sync.
	if w.sequence == 0 && sequence != 1 {
		return 0, ErrWalOutOfSync
	}

	// Handle writes that that should be queued to be written later
	if w.sequence != 0 && sequence > w.sequence+1 {
		w.writeQueue = append(w.writeQueue, WalWrite{
			Data:      p,
			Offset:    off,
			Sequence:  sequence,
			Timestamp: timestamp,
		})

		return 0, nil
	}

	// Handle items that should be ignored, could be duplicated or out-of-order
	if w.sequence != 0 && sequence <= w.sequence {
		return 0, ErrWalSequenceMismatch
	}

	n, err = w.file.WriteAt(p, off)

	if err != nil {
		return n, err
	}

	w.sequence = sequence
	w.timestamp = timestamp

	// Apply items that were queued to be written
	w.applyWriteQueue()

	return n, err
}
