package storage

import (
	"encoding/binary"
	"fmt"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"os"
)

type DatabaseMetadata struct {
	BranchUuid         string `json:"branch_uuid"`
	DatabaseUuid       string `json:"database_uuid"`
	databaseFileSystem DatabaseFileSystem
	file               internalStorage.File
	PageCount          int64
	PageSize           int64
}

func NewDatabaseMetadata(dfs DatabaseFileSystem, databaseUuid, branchUuid string) (*DatabaseMetadata, error) {
	var err error

	metadata := &DatabaseMetadata{
		BranchUuid:         branchUuid,
		DatabaseUuid:       databaseUuid,
		databaseFileSystem: dfs,
		PageCount:          0,
		PageSize:           dfs.PageSize(),
	}

	metadata.file, err = dfs.FileSystem().OpenFile(metadata.Path(), os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err := dfs.FileSystem().MkdirAll(file.GetDatabaseFileDir(databaseUuid, branchUuid), 0755)

			if err != nil {
				return nil, err
			}

			metadata.file, err = dfs.FileSystem().OpenFile(metadata.Path(), os.O_CREATE|os.O_RDWR, 0644)

			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	err = metadata.Load()

	if err != nil {
		metadata.PageCount = 0
	}

	return metadata, nil
}

// Load the database metadata
func (d *DatabaseMetadata) Load() error {
	// Read the first 8 bytes to get the page count
	data := make([]byte, 8)

	_, err := d.file.Read(data)

	if err != nil {
		return err
	}

	d.PageCount = int64(binary.LittleEndian.Uint64(data))

	return nil
}

func (d *DatabaseMetadata) FileSize() int64 {
	return d.PageCount * d.PageSize
}

func (d *DatabaseMetadata) Path() string {
	return fmt.Sprintf("%s/_METADATA", file.GetDatabaseFileDir(d.DatabaseUuid, d.BranchUuid))
}

// Save the database meta data
func (d *DatabaseMetadata) Save() error {
	data := make([]byte, 8)

	// Write the page count
	binary.LittleEndian.PutUint64(data, uint64(d.PageCount))

	return d.databaseFileSystem.FileSystem().WriteFile(d.Path(), data, 0644)
}

// Increment the page count
func (d *DatabaseMetadata) SetPageCount(count int64) error {
	d.PageCount = count

	return d.Save()
}
