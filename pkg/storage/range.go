package storage

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/litebase/litebase/pkg/file"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

/*
A range represents a subset of the data in a database. It is used to split
the database into smaller files to allow the database to scale to larger sizes
that typically would not be possible with a single file.
*/

const (
	RangeVersion  int32 = 1
	RangeMaxPages int64 = 4096
)

// OPTIMIZE: Track range size to avoid unnecessary file i/o operations. For
// example, searching for a page in the range that may not exist.
type Range struct {
	branchId   string
	databaseId string
	closed     bool
	file       internalStorage.File
	fs         *FileSystem
	number     int64
	pageSize   int64
	Timestamp  int64
}

// NewRange creates a new range for the specified path.
func NewRange(databaseId, branchId string, fs *FileSystem, rangeNumber int64, pageSize int64, timestamp int64) (*Range, error) {
	dr := &Range{
		branchId:   branchId,
		databaseId: databaseId,
		fs:         fs,
		pageSize:   pageSize,
		number:     rangeNumber,
		Timestamp:  timestamp,
	}

tryOpen:
	file, err := fs.OpenFile(dr.Path(), os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = fs.MkdirAll(filepath.Dir(dr.Path()), 0750)

			if err != nil {
				log.Println("Error creating range directory", err)
				return nil, err
			}

			goto tryOpen
		} else {
			log.Println("Error opening range file", err)
			return nil, err
		}
	}

	dr.file = file

	return dr, nil
}

// Close the range file.
func (dr *Range) Close() error {
	if dr.closed {
		return nil
	}

	dr.file.Close()

	dr.closed = true

	return nil
}

// Delete the range file from disk.
func (dr *Range) Delete() error {
	err := dr.fs.Remove(dr.Path())

	if err != nil {
		log.Println("Error removing range file", err)

		return err
	}

	return nil
}

// The unique identifier for the range file.
func (dr *Range) ID() string {
	return fmt.Sprintf("%010d_%d", dr.number, dr.Timestamp)
}

// The number of pages in the range file.
func (dr *Range) PageCount() int64 {
	if dr.closed {
		return 0
	}

	size, err := dr.Size()

	if err != nil {
		log.Println("Error getting range file size", err)
		return 0
	}

	return size / dr.pageSize
}

// The path to the range file.
func (r *Range) Path() string {
	return fmt.Sprintf(
		"%s%s",
		file.GetDatabaseFileDir(r.databaseId, r.branchId),
		r.ID(),
	)
}

// Perform a read operation at the specified page number.
func (dr *Range) ReadAt(pageNumber int64, p []byte) (n int, err error) {
	if dr.closed {
		return 0, os.ErrClosed
	}

	offset := file.PageRangeOffset(pageNumber, RangeMaxPages, dr.pageSize)

	// Read the data from the range file
	n, err = dr.file.ReadAt(p, offset)

	if err != nil {
		if err == io.EOF {
			return n, nil
		}

		log.Println("Error reading range file", err)

		return 0, err
	}

	return n, nil
}

// Return the size of the range file in bytes.
func (dr *Range) Size() (int64, error) {
	if dr.closed {
		return 0, os.ErrClosed
	}

	stat, err := dr.file.Stat()

	if err != nil {
		log.Println("Error getting file size", err)
		return 0, err
	}

	size := stat.Size()

	pageCount := size / dr.pageSize

	return pageCount * (dr.pageSize), nil
}

// Truncate the range file to the specified size in bytes.
func (dr *Range) Truncate(size int64) error {
	if dr.closed {
		return os.ErrClosed
	}

	err := dr.file.Truncate(size)

	if err != nil {
		log.Println("Error truncating range file", err)

		return err
	}

	return nil
}

// Perform a write operation at the specified page number.
func (dr *Range) WriteAt(pageNumber int64, p []byte) (n int, err error) {
	if dr.closed {
		return 0, os.ErrClosed
	}

	offset := file.PageRangeOffset(pageNumber, RangeMaxPages, dr.pageSize)

	// Write the data to the range file
	n, err = dr.file.WriteAt(p, offset)

	if err != nil {
		log.Println("Error writing to range file", err)
		return 0, err
	}

	return n, nil
}
