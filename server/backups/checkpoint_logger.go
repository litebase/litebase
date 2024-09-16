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

type CheckpointLogger struct {
	BranchUuid   string
	DatabaseUuid string
	file         internalStorage.File
}

// The CheckpointLogger is responsible for logging Snapshots to a file when the
// database is checkpointed. Each log entry contains a timestamp and the number
// of pages that were written to the snapshot.
func NewCheckpointLogger(databaseUuid, branchUuid string) *CheckpointLogger {
	return &CheckpointLogger{
		BranchUuid:   branchUuid,
		DatabaseUuid: databaseUuid,
	}
}

func (c *CheckpointLogger) Close() error {
	if c.file != nil {
		return c.file.Close()
	}

	return nil
}

func (c *CheckpointLogger) File() (internalStorage.File, error) {
	if c.file != nil {
		return c.file, nil
	}

openFile:
	directory := file.GetDatabaseFileBaseDir(c.DatabaseUuid, c.BranchUuid)
	path := GetSnapshotPath(c.DatabaseUuid, c.BranchUuid)
	file, err := storage.TieredFS().OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

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

func (c *CheckpointLogger) Log(timestamp uint64, pageCount uint32) error {
	file, err := c.File()

	if err != nil {
		return err
	}

	file.Seek(0, io.SeekEnd)

	data := make([]byte, 64)
	binary.LittleEndian.PutUint64(data[0:8], timestamp)
	binary.LittleEndian.PutUint32(data[8:12], pageCount)

	_, err = file.Write(data)

	return err
}
