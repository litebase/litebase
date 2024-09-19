package backups

import (
	"encoding/binary"
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/storage"
	"os"
)

type SnapshotLogger struct {
	BranchUuid   string
	DatabaseUuid string
	file         internalStorage.File
}

// The SnapshotLogger is responsible for logging Snapshots to a file when the
// database is Snapshotted. Each log entry contains a timestamp and the number
// of pages that were written to the snapshot.
func NewSnapshotLogger(databaseUuid, branchUuid string) *SnapshotLogger {
	return &SnapshotLogger{
		BranchUuid:   branchUuid,
		DatabaseUuid: databaseUuid,
	}
}

// Close the logger and the underlying file.
func (c *SnapshotLogger) Close() error {
	if c.file != nil {
		return c.file.Close()
	}

	return nil
}

// Get the snapshot log file, creating it if it does not exist.
func (c *SnapshotLogger) File() (internalStorage.File, error) {
	if c.file != nil {
		return c.file, nil
	}

openFile:
	snapshotFile, err := storage.TieredFS().OpenFile(GetSnapshotPath(c.DatabaseUuid, c.BranchUuid), SNAPSHOT_LOG_FLAGS, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err := storage.TieredFS().MkdirAll(fmt.Sprintf("%s/logs/snapshots", file.GetDatabaseFileBaseDir(c.DatabaseUuid, c.BranchUuid)), 0755)

			if err != nil {
				return nil, err
			}

			goto openFile
		}

		return nil, err
	}

	c.file = snapshotFile

	return c.file, nil
}

// Write a snapshot log entry to the snapshot log file.
func (c *SnapshotLogger) Log(timestamp, pageCount int64) error {
	file, err := c.File()

	if err != nil {
		return err
	}

	_, err = file.Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	data := make([]byte, 64)
	binary.LittleEndian.PutUint64(data[0:8], uint64(timestamp))
	binary.LittleEndian.PutUint32(data[8:12], uint32(pageCount))

	_, err = file.Write(data)

	return err
}
