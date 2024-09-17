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

func (c *SnapshotLogger) Close() error {
	if c.file != nil {
		return c.file.Close()
	}

	return nil
}

func (c *SnapshotLogger) File() (internalStorage.File, error) {
	if c.file != nil {
		return c.file, nil
	}

openFile:
	directory := file.GetDatabaseFileBaseDir(c.DatabaseUuid, c.BranchUuid)
	path := GetSnapshotPath(c.DatabaseUuid, c.BranchUuid)
	file, err := storage.TieredFS().OpenFile(path, SNAPSHOT_LOG_FLAGS, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = storage.TieredFS().MkdirAll(fmt.Sprintf("%s/logs/snapshots", directory), 0755)

			if err != nil {
				return nil, err
			}

			goto openFile
		}

		return nil, err
	}

	c.file = file

	return c.file, nil
}

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
