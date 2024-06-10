package backups

import (
	"encoding/binary"
	"fmt"
	"io"
	"litebasedb/server/file"
	"os"
)

type CheckpointLogger struct {
	branchUuid   string
	databaseUuid string
	file         *os.File
}

func NewCheckpointLogger(databaseUuid, branchUuid string) *CheckpointLogger {
	return &CheckpointLogger{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
	}
}

func (c *CheckpointLogger) File() (*os.File, error) {
	if c.file != nil {
		return c.file, nil
	}

openFile:
	directory := file.GetDatabaseFileDir(c.databaseUuid, c.branchUuid)
	path := fmt.Sprintf("%s/logs/snapshots", directory)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(fmt.Sprintf("%s/logs", directory), 0755)

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
