package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"os"
	"sync"
)

type DatabaseMetadata struct {
	BranchUuid         string `json:"branch_uuid"`
	DatabaseUuid       string `json:"database_uuid"`
	databaseFileSystem *DurableDatabaseFileSystem
	file               internalStorage.File
	mutext             sync.Mutex
	PageCount          int64
	PageSize           int64
}

func NewDatabaseMetadata(dfs *DurableDatabaseFileSystem, databaseUuid, branchUuid string) (*DatabaseMetadata, error) {
	var err error

	metadata := &DatabaseMetadata{
		BranchUuid:         branchUuid,
		DatabaseUuid:       databaseUuid,
		databaseFileSystem: dfs,
		mutext:             sync.Mutex{},
		PageCount:          0,
		PageSize:           dfs.PageSize(),
	}

	err = metadata.Load()

	if err != nil {
		metadata.PageCount = 0
	}

	return metadata, nil
}

func (d *DatabaseMetadata) Close() error {
	if d.file == nil {
		return nil
	}

	err := d.file.Close()

	d.file = nil

	return err
}

func (d *DatabaseMetadata) File() (internalStorage.File, error) {
	var err error

	d.mutext.Lock()
	defer d.mutext.Unlock()

	if d.file == nil {
		d.file, err = d.databaseFileSystem.FileSystem().OpenFile(d.Path(), os.O_CREATE|os.O_RDWR, 0644)

		if err != nil {
			if os.IsNotExist(err) {
				err := d.databaseFileSystem.FileSystem().MkdirAll(file.GetDatabaseFileDir(d.DatabaseUuid, d.BranchUuid), 0755)

				if err != nil {
					return nil, err
				}

				d.file, err = d.databaseFileSystem.FileSystem().OpenFile(d.Path(), os.O_CREATE|os.O_RDWR, 0644)

				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
	}

	return d.file, nil
}

func (d *DatabaseMetadata) FileSize() int64 {
	return d.PageCount * d.PageSize
}

// Load the database metadata
func (d *DatabaseMetadata) Load() error {
	// Read the first 8 bytes to get the page count
	data := make([]byte, 8)

	file, err := d.File()

	if err != nil {
		return err
	}

	_, err = file.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	file, err = d.File()

	if err != nil {
		return err
	}

	_, err = file.Read(data)

	if err != nil {
		return err
	}

	d.PageCount = int64(binary.LittleEndian.Uint64(data))

	return nil
}

func (d *DatabaseMetadata) Path() string {
	return fmt.Sprintf("%s/_METADATA", file.GetDatabaseFileDir(d.DatabaseUuid, d.BranchUuid))
}

// Save the database meta data
func (d *DatabaseMetadata) Save() error {
	data := make([]byte, 8)

	// Write the page count
	binary.LittleEndian.PutUint64(data, uint64(d.PageCount))

	file, err := d.File()

	if err != nil {
		return err
	}

	_, err = file.WriteAt(data, 0)

	return err

}

// Update the page count
func (d *DatabaseMetadata) SetPageCount(count int64) error {
	d.PageCount = count

	return d.Save()
}
