package storage

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	pageSize   int64
	path       string
	number     int64
}

// NewRange creates a new range for the specified path.
func NewRange(databaseId, branchId string, fs *FileSystem, path string, rangeNumber int64, pageSize int64) (*Range, error) {
	dr := &Range{
		branchId:   branchId,
		databaseId: databaseId,
		fs:         fs,
		pageSize:   pageSize,
		path:       path,
		number:     rangeNumber,
	}

tryOpen:
	file, err := fs.OpenFile(dr.getPath(), os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = fs.MkdirAll(filepath.Dir(dr.getPath()), 0750)

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

func (dr *Range) Close() error {
	err := dr.file.Close()

	if err != nil {
		log.Println("Error closing range file", err)
		return err
	}

	dr.closed = true

	return nil
}

func (dr *Range) Delete() error {
	err := dr.file.Close()

	if err != nil {
		log.Println("Error closing range file", err)

		return err
	}

	err = dr.fs.Remove(dr.getPath())

	if err != nil {
		log.Println("Error removing range file", err)

		return err
	}

	return nil
}

func (dr *Range) getPath() string {
	var builder strings.Builder
	builder.Grow(len(dr.path) + 10) // Preallocate memory
	builder.WriteString(dr.path)

	// Create a strings.Builder for efficient string concatenation
	var pageNumberBuilder strings.Builder
	pageNumberBuilder.Grow(10) // Preallocate memory for 10 characters

	// Convert rangeNumber to a zero-padded 10-digit string
	rangeStr := strconv.FormatInt(dr.number, 10)
	padding := 10 - len(rangeStr)

	for i := 0; i < padding; i++ {
		pageNumberBuilder.WriteByte('0')
	}

	pageNumberBuilder.WriteString(rangeStr)

	builder.WriteString(pageNumberBuilder.String())

	return builder.String()
}

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
