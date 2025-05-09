package backups

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"sort"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

type RollbackLogIdentifier uint32

const (
	RollbackLogFrameID RollbackLogIdentifier = 0x01
	RollbackLogEntryID RollbackLogIdentifier = 0x02
)

// The RollbackLog is a data structure used to keep track of database page changes
// that occur at given point in time. Each RollbackLog file contains multiple
// RollbackLogEntries which are used to store the state of pages before they are
// modified. In the event of a database restore, the RollbackLog is used to
// retrieve the page version that meets the restore criteria.
type RollbackLog struct {
	File      internalStorage.File
	mutex     sync.Mutex
	Timestamp int64
}

// Open the right rollback log file for the given database and branch. If the
// file does not exist, it will be created.
func OpenRollbackLog(tierdFS *storage.FileSystem, databaseId, branchId string, timestamp int64) (*RollbackLog, error) {
log:
	directory := file.GetDatabaseRollbackDirectory(databaseId, branchId)
	path := fmt.Sprintf("%s/%d", directory, timestamp)
	file, err := tierdFS.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = tierdFS.MkdirAll(directory, 0755)

			if err != nil {
				return nil, err
			}

			goto log
		}

		return nil, err
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

	// _, err = r.File.Seek(offset, io.SeekStart)

	// if err != nil {
	// 	return err
	// }

	data, err = frame.Serialize()

	if err != nil {
		return err
	}

	// _, err = r.File.Write(data)
	// log.Println("Writing frame entry at offset", offset, "with size", size)
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
			offset, err = r.File.Seek(offset, io.SeekStart)

			if err != nil {
				log.Println("Error seeking file:", err)
				errorChannel <- err
				return
			}

			n, err := r.File.Read(frameEntryData)

			if err == io.EOF {
				break
			}

			offset += int64(n)

			if err != nil {
				log.Println("Error reading frame entry:", err)
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
