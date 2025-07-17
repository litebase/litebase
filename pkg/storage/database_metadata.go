package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/litebase/litebase/pkg/file"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/internal/utils"
)

type DatabaseMetadata struct {
	BranchID           string `json:"branch_id"`
	DatabaseID         string `json:"database_id"`
	databaseFileSystem *DurableDatabaseFileSystem
	file               internalStorage.File
	mutext             sync.Mutex
	PageCount          int64
	PageSize           int64
}

func NewDatabaseMetadata(dfs *DurableDatabaseFileSystem, databaseId, branchId string) (*DatabaseMetadata, error) {
	var err error

	metadata := &DatabaseMetadata{
		BranchID:           branchId,
		DatabaseID:         databaseId,
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

// Return the file associated with the database metadata, opening it if necessary.
func (d *DatabaseMetadata) File() (internalStorage.File, error) {
	var err error

	d.mutext.Lock()
	defer d.mutext.Unlock()

	if d.file == nil {
	tryOpen:
		d.file, err = d.databaseFileSystem.FileSystem().OpenFile(d.Path(), os.O_CREATE|os.O_RDWR, 0600)

		if err != nil {
			if os.IsNotExist(err) {
				err := d.databaseFileSystem.FileSystem().MkdirAll(file.GetDatabaseFileDir(d.DatabaseID, d.BranchID), 0750)

				if err != nil {
					return nil, err
				}

				goto tryOpen
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

	pageCountInt64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data))

	if err != nil {
		slog.Error("Error decoding database metadata page count", "error", err)
		return err
	}

	d.PageCount = pageCountInt64

	return nil
}

func (d *DatabaseMetadata) Path() string {
	return fmt.Sprintf("%s_METADATA", file.GetDatabaseFileDir(d.DatabaseID, d.BranchID))
}

// Save the database meta data
func (d *DatabaseMetadata) Save() error {
	data := make([]byte, 8)

	// Write the page count
	uint64PageCount, err := utils.SafeInt64ToUint64(d.PageCount)

	if err != nil {
		slog.Error("Error encoding database metadata page count", "error", err)
		return err
	}

	binary.LittleEndian.PutUint64(data, uint64PageCount)

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
