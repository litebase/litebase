package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/file"
)

type DataRangeIndex struct {
	drm  *DataRangeManager
	file internalStorage.File
}

// Create a new instance of the data range index.
func NewDataRangeIndex(drm *DataRangeManager) *DataRangeIndex {
	return &DataRangeIndex{
		drm: drm,
	}
}

// Close the data range index file if it is open.
func (dri *DataRangeIndex) Close() error {
	if dri.file == nil {
		return nil
	}

	err := dri.file.Close()

	dri.file = nil

	return err
}

// Return the file associated with the data range index, opening it if necessary.
func (dri *DataRangeIndex) File() (internalStorage.File, error) {
	var err error

	if dri.file == nil {
	tryOpen:
		dri.file, err = dri.drm.dfs.FileSystem().OpenFile(dri.Path(), os.O_CREATE|os.O_RDWR, 0600)

		if err != nil {
			if os.IsNotExist(err) {
				err := dri.drm.dfs.FileSystem().MkdirAll(file.GetDatabaseFileDir(dri.drm.dfs.databaseId, dri.drm.dfs.branchId), 0750)

				if err != nil {
					return nil, err
				}

				goto tryOpen
			} else {
				return nil, err
			}
		}
	}

	return dri.file, nil
}

// Return the version of the specified range number from the index file.
func (dri *DataRangeIndex) Get(rangeNumber int64) (bool, int64, error) {
	var err error
	var rangeVersion int64

	file, err := dri.File()

	if err != nil {
		return false, 0, err
	}

	// The position in the index file is (rangeNumber - 1) * 8 because range
	// numbers start at 1 and each entry is 8 bytes (int64).
	offset := (rangeNumber - 1) * 8

	// Read the range offset from the index file
	_, err = file.Seek(offset, io.SeekStart)

	if err != nil {
		if err == io.EOF {
			return false, 0, nil
		}

		return false, 0, err
	}

	err = binary.Read(file, binary.LittleEndian, &rangeVersion)

	if err != nil {
		if err == io.EOF {
			return false, 0, nil
		}

		return false, 0, err
	}

	return true, rangeVersion, nil
}

// Return the path of the data range index file.
func (dri *DataRangeIndex) Path() string {
	return fmt.Sprintf("%s_RANGE_INDEX", file.GetDatabaseFileDir(dri.drm.dfs.databaseId, dri.drm.dfs.branchId))
}

// Set the version of the specified range number in the index file.
func (dri *DataRangeIndex) Set(rangeNumber int64, rangeVersion int64) error {
	var err error

	file, err := dri.File()

	if err != nil {
		return err
	}

	// The position in the index file is (rangeNumber - 1) * 8 because range
	// numbers start at 1 and each entry is 8 bytes (int64).
	offset := (rangeNumber - 1) * 8

	// Write the range version directly at the offset
	buf := make([]byte, 8)

	binary.LittleEndian.PutUint64(buf, uint64(rangeVersion))

	_, err = file.WriteAt(buf, offset)

	if err != nil {
		return err
	}

	return nil
}
