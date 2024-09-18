package backups

import (
	"bytes"
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
	"slices"
	"sort"
)

type RollbackLogIdentifier uint32

const (
	RollbackLogFrameID RollbackLogIdentifier = 0x01
	// TODO: need to use this in the RollkbacLogEntry
	RollbackLogEntryID RollbackLogIdentifier = 0x02
)

/*
The RollbackLog is a data structure used to keep track of database page changes
that occur at given point in time. Each RollbackLog file contains multiple
RollbackLogEntries which are used to store the state of pages before they are
modified. In the event of a database restore, the RollbackLog is used to
retrieve the page version that meets the restore criteria.
*/

type RollbackLog struct {
	File      internalStorage.File
	Timestamp int64
}

func OpenRollbackLog(databaseUuid, branchUuid string, timestamp int64) (*RollbackLog, error) {
log:
	directory := file.GetDatabaseRollbackDirectory(databaseUuid, branchUuid)
	path := fmt.Sprintf("%s/%d", directory, timestamp)
	file, err := storage.TieredFS().OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = storage.TieredFS().MkdirAll(directory, 0755)

			if err != nil {
				return nil, err
			}

			goto log
		}

		return nil, err
	}

	return &RollbackLog{
		File:      file,
		Timestamp: timestamp,
	}, nil
}

func (r *RollbackLog) AppendFrame(timestamp int64) (offset int64, size int64, err error) {
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

func (r *RollbackLog) AppendLog(compressionBuffer *bytes.Buffer, entry *RollbackLogEntry) (size int64, err error) {
	_, err = r.File.Seek(0, io.SeekEnd)

	if err != nil {
		return 0, err
	}

	serialized, err := entry.Serialize(compressionBuffer)

	if err != nil {
		return 0, err
	}

	_, err = r.File.Write(serialized)

	return int64(len(serialized)), err
}

func (r *RollbackLog) Close() error {
	return r.File.Close()
}

func (r *RollbackLog) Commit(offset int64, size int64) error {
	_, err := r.File.Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	// Read the frame entry
	data := make([]byte, RollbackFrameHeaderSize)

	_, err = r.File.Seek(offset, io.SeekStart)

	if err != nil {
		return err
	}

	_, err = r.File.Read(data)

	if err != nil {
		return err
	}

	frame, err := DeserializeRollbackLogFrame(data)

	if err != nil {
		return err
	}

	// Update the frame entry with the new offset
	frame.Committed = 1
	frame.Size = size

	_, err = r.File.Seek(offset, io.SeekStart)

	if err != nil {
		return err
	}

	data, err = frame.Serialize()

	if err != nil {
		return err
	}

	_, err = r.File.Write(data)

	return err
}

// TODO: use a channel or some sort of io copy to read the log entries so we can
// read them and immediately process them without having to store them in memory.
func (r *RollbackLog) ReadAfter(timestamp int64) ([][]*RollbackLogEntry, error) {
	index := make(map[int64][]RollbackLogFrame)

	// Reset the file pointer to the start of the file
	_, err := r.File.Seek(0, io.SeekStart)

	if err != nil {
		log.Println("Error seeking file:", err)
		return nil, err
	}

	// Loop through the frames in the rollback log and find frames that are
	// greater than the timestamp specified
	frameEntryData := make([]byte, RollbackFrameHeaderSize)

	for {
		_, err := r.File.Read(frameEntryData)

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Println("Error reading frame entry:", err)
			return nil, err
		}

		frame, err := DeserializeRollbackLogFrame(frameEntryData)

		if err != nil {
			return nil, err
		}

		if frame.Timestamp >= timestamp {
			if _, ok := index[frame.Timestamp]; !ok {
				index[frame.Timestamp] = []RollbackLogFrame{frame}
			} else {
				index[frame.Timestamp] = append(index[frame.Timestamp], frame)
			}
		}

		_, err = r.File.Seek(frame.Offset+frame.Size, io.SeekStart)

		if err != nil {
			log.Println("Error seeking to next frame:", err)
			return nil, err
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

	frames := make([][]*RollbackLogEntry, 0)

	i := 0

	// Frames are segmented by timestamp, but each frame should be treated
	// as a separate entry to properly read the log entries in reverse order.
	for _, key := range indexKeys {
		frameEntries := index[key]

		slices.Reverse(frameEntries)

		// Enter the frame and read the log entries
		for _, frame := range frameEntries {
			frames = append(frames, make([]*RollbackLogEntry, 0))

			_, err := r.File.Seek(frame.Offset+RollbackFrameHeaderSize, io.SeekStart)

			if err != nil {
				log.Println("Error seeking to frame offset:", err)
				return nil, err
			}

			frameSize := frame.Size - RollbackFrameHeaderSize

			for frameSize > 0 {
				entry, err := DeserializeRollbackLogEntry(r.File)

				if err != nil {
					log.Println("Error deserializing rollback log entry:", err)
					return nil, err
				}

				frameSize -= int64(RollbackLogEntryHeaderSize + entry.SizeCompressed)

				frames[i] = append(frames[i], entry)
			}

			// Sort the pages in the frame by page number in descending order
			slices.Reverse(frames[i])

			i++
		}
	}

	return frames, nil
}

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
