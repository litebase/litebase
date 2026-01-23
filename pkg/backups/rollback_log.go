package backups

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"slices"
	"sort"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

type RollbackLogIdentifier uint32

const (
	RollbackLogFrameID RollbackLogIdentifier = 0x01
	RollbackLogEntryID RollbackLogIdentifier = 0x02
)

// GetRollbackLogPath returns the file path for a rollback log.
func GetRollbackLogPath(databaseId, branchId string, timestamp int64) string {
	return fmt.Sprintf(
		"%s/%d",
		file.GetDatabaseRollbackDirectory(databaseId, branchId),
		timestamp,
	)
}

// The RollbackLog is a data structure used to keep track of database page changes
// that occur at given point in time. Each RollbackLog file contains multiple
// RollbackLogEntries which are used to store the state of pages before they are
// modified. In the event of a database restore, the RollbackLog is used to
// retrieve the page version that meets the restore criteria.
type RollbackLog struct {
	dataKey   []byte // Optional: 32-byte encryption key
	encrypted bool   // Whether this rollback log is encrypted
	File      internalStorage.File
	keyHash   [32]byte // Optional: SHA256 hash of encryption key
	mutex     sync.Mutex
	Timestamp int64
}

// Open the right rollback log file for the given database and branch. If the
// file does not exist, it will be created.
func OpenRollbackLog(tierdFS *storage.FileSystem, databaseId, branchId string, timestamp int64) (*RollbackLog, error) {
	return OpenRollbackLogWithEncryption(tierdFS, databaseId, branchId, timestamp, nil, [32]byte{})
}

// OpenRollbackLogWithEncryption opens a rollback log with optional encryption.
func OpenRollbackLogWithEncryption(tierdFS *storage.FileSystem, databaseId, branchId string, timestamp int64, dataKey []byte, keyHash [32]byte) (*RollbackLog, error) {
openLog:
	directory := file.GetDatabaseRollbackDirectory(databaseId, branchId)
	path := fmt.Sprintf("%s/%d", directory, timestamp)
	file, err := tierdFS.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = tierdFS.MkdirAll(directory, 0750)

			if err != nil {
				return nil, err
			}

			goto openLog
		}

		return nil, err
	}

	// Wrap with encryption if enabled
	if len(dataKey) == 32 {
		fileInfo, err := file.Stat()

		if err != nil {
			if closeErr := file.Close(); closeErr != nil {
				slog.Error("Error closing rollback log after stat error:", "error", closeErr)
			}

			return nil, fmt.Errorf("failed to stat rollback log: %w", err)
		}

		// Use portable encryption path for rollback logs
		encryptionPath := fmt.Sprintf("database/%s/rollback/%d", databaseId, timestamp)

		if fileInfo.Size() == 0 {
			// New file - create encrypted wrapper
			encryptedFile, err := storage.NewEncryptedStreamFile(file, dataKey, keyHash, 0, encryptionPath)

			if err != nil {
				if closeErr := file.Close(); closeErr != nil {
					slog.Error("Error closing rollback log after encryption error:", "error", closeErr)
				}

				return nil, fmt.Errorf("failed to create encrypted rollback log: %w", err)
			}

			// Write the header
			if err := encryptedFile.WriteHeader(); err != nil {
				if closeErr := file.Close(); closeErr != nil {
					slog.Error("Error closing rollback log after header write error:", "error", closeErr)
				}

				return nil, fmt.Errorf("failed to write encrypted rollback log header: %w", err)
			}

			return &RollbackLog{
				dataKey:   dataKey,
				encrypted: true,
				File:      encryptedFile,
				keyHash:   keyHash,
				mutex:     sync.Mutex{},
				Timestamp: timestamp,
			}, nil
		} else {
			// Existing file - open encrypted wrapper
			encryptedFile, err := storage.OpenEncryptedStreamFile(file, dataKey, keyHash, 0, encryptionPath)

			if err != nil {
				if closeErr := file.Close(); closeErr != nil {
					slog.Error("Error closing rollback log after encryption open error:", "error", closeErr)
				}

				return nil, fmt.Errorf("failed to open encrypted rollback log: %w", err)
			}

			return &RollbackLog{
				dataKey:   dataKey,
				encrypted: true,
				File:      encryptedFile,
				keyHash:   keyHash,
				mutex:     sync.Mutex{},
				Timestamp: timestamp,
			}, nil
		}
	}

	return &RollbackLog{
		File:      file,
		mutex:     sync.Mutex{},
		Timestamp: timestamp,
	}, nil
}

// Append a new frame to the rollback log and return the offset and size of the
// frame to the caller.
func (r *RollbackLog) AppendFrame(timestamp int64) (offset int64, size int64, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	offset, err = r.File.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, 0, err
	}

	rollbackFrameEntry := RollbackLogFrame{
		Committed: 0,
		Timestamp: timestamp,
		Offset:    offset,
		Size:      0,
	}

	data, err := rollbackFrameEntry.Serialize()

	if err != nil {
		return 0, 0, err
	}

	_, err = r.File.Write(data)

	if err != nil {
		return 0, 0, err
	}

	return offset, int64(len(data)), nil
}

// Append a new log entry to the rollback log and return the size of the entry
// to the caller.
func (r *RollbackLog) AppendLog(compressionBuffer *bytes.Buffer, entry *RollbackLogEntry) (size int64, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	_, err = r.File.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, err
	}

	serialized, err := entry.Serialize(compressionBuffer)

	if err != nil {
		return 0, err
	}

	n, err := r.File.Write(serialized)

	return int64(n), err
}

// Close the rollback log and the underlying file.
func (r *RollbackLog) Close() error {
	if r.File == nil {
		return nil
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.File.Close()
}

// Commit the current frame in the rollback log.
func (r *RollbackLog) Commit(offset int64, size int64) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Read the frame entry
	data := make([]byte, RollbackFrameHeaderSize)

	_, err := r.File.ReadAt(data, offset)

	if err != nil {
		log.Println("Error reading frame entry:", err)

		return err
	}

	frame, err := DeserializeRollbackLogFrame(data)

	if err != nil {
		log.Println("Error deserializing frame entry:", err)
		// If we are unable to deserialize the frame entry, we should not
		// continue with the commit operation. In fact the whole program should
		// panic because this is a critical error. The rollback log is corrupted
		// and we cannot continue.
		return err
	}

	// Update the frame entry with the new offset
	frame.Committed = 1
	frame.Size = size

	data, err = frame.Serialize()

	if err != nil {
		return err
	}

	_, err = r.File.WriteAt(data, offset)

	return err
}

// Read the rollback log entries that occurred at or after the specified
// timestamp and return them on a channel.
func (r *RollbackLog) ReadForTimestamp(timestamp int64) (
	rollbackLogEntriesChannel chan []*RollbackLogEntry,
	doneChannel chan struct{},
	errorChannel chan error,
) {
	// Create a channel to send the log entries
	rollbackLogEntriesChannel = make(chan []*RollbackLogEntry)
	doneChannel = make(chan struct{})
	errorChannel = make(chan error)

	go func() {
		r.mutex.Lock()
		defer r.mutex.Unlock()

		index := make(map[int64][]RollbackLogFrame)

		// Reset the file pointer to the start of the file
		_, err := r.File.Seek(0, io.SeekStart)

		if err != nil {
			log.Println("Error seeking file:", err)
			errorChannel <- err
			return
		}

		// Loop through the frames in the rollback log and find frames that are
		// greater than the timestamp specified
		frameEntryData := make([]byte, RollbackFrameHeaderSize)
		offset := int64(0)

		for {
			// Reset the file pointer to the start of the file
			_, err = r.File.Seek(offset, io.SeekStart)

			if err != nil {
				log.Println("Error seeking file:", err)
				errorChannel <- err
				return
			}

			_, err := r.File.Read(frameEntryData)

			if err == io.EOF {
				break
			}

			if err != nil {
				slog.Error("Error reading frame entry:", "error", err)
				errorChannel <- err
				return
			}

			frame, err := DeserializeRollbackLogFrame(frameEntryData)

			if err != nil {
				log.Println("Error deserializing frame entry:", err)
				errorChannel <- err
				return
			}

			if frame.Timestamp >= timestamp {
				if _, ok := index[frame.Timestamp]; !ok {
					index[frame.Timestamp] = []RollbackLogFrame{frame}
				} else {
					index[frame.Timestamp] = append(index[frame.Timestamp], frame)
				}
			}

			offset, err = r.File.Seek(frame.Offset+frame.Size, io.SeekStart)

			if err != nil {
				log.Println("Error seeking to next frame:", err)
				errorChannel <- err
				return
			}
		}

		indexKeys := make([]int64, 0, len(index))

		for k := range index {
			indexKeys = append(indexKeys, k)
		}

		// Sort the keys in descending order
		sort.Slice(indexKeys, func(i, j int) bool {
			return indexKeys[i] > indexKeys[j]
		})

		// Frames are segmented by timestamp, but each frame should be treated
		// as a separate entry to properly read the log entries in reverse order.
		for _, key := range indexKeys {
			frameEntries := index[key]

			slices.Reverse(frameEntries)

			// Enter the frame and read the log entries
			for _, frame := range frameEntries {
				rollbackLogEntries := make([]*RollbackLogEntry, 0)

				_, err := r.File.Seek(frame.Offset+RollbackFrameHeaderSize, io.SeekStart)

				if err != nil {
					log.Println("Error seeking to frame offset:", err)
					errorChannel <- err
					return
				}

				frameSize := frame.Size - RollbackFrameHeaderSize

				for frameSize > 0 {
					entry, err := DeserializeRollbackLogEntry(r.File)

					if err != nil {
						log.Println("Error deserializing rollback log entry:", err)
						errorChannel <- err
						return
					}

					frameSize -= int64(RollbackLogEntryHeaderSize + entry.SizeCompressed)

					rollbackLogEntries = append(rollbackLogEntries, entry)
				}

				// Sort the pages in the frame by page number in descending order
				slices.Reverse(rollbackLogEntries)

				rollbackLogEntriesChannel <- rollbackLogEntries
			}
		}

		doneChannel <- struct{}{}
	}()

	return
}

// Rollback the log frame that has been written to the file at the specified
// offset and size.
func (r *RollbackLog) Rollback(offset, size int64) error {
	// Determine if the offset and size are at the end of the file
	fileInfo, err := r.File.Stat()

	if err != nil {
		return err
	}

	// Ensure we are rolling back entries that are only at the end of the file
	if offset+size != fileInfo.Size() {
		return fmt.Errorf("the log entries cannot be rolled back, offset and size do not match the end of the file")
	}

	// Truncate the file to the specified offset
	err = r.File.Truncate(offset)

	if err != nil {
		return err
	}

	return nil
}
