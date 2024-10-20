package storage

import (
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

/*
A data range represents a subset of the data in a database. It is used to split
the database into smaller files to allow the database to scale to larger sizes
that typically would not be possible with a single file.
*/

const (
	DataRangeHeaderSize int64 = 100
	DataRangeVersion    int32 = 1
	DataRangeMaxPages   int64 = 4096
)

type DataRange struct {
	closed   bool
	file     internalStorage.File
	fs       *FileSystem
	pageSize int64
	path     string
	number   int64
}

// NewDataRange creates a new data range for the specified path.
func NewDataRange(fs *FileSystem, path string, rangeNumber int64, pageSize int64) (*DataRange, error) {
	dr := &DataRange{
		fs:       fs,
		pageSize: pageSize,
		path:     path,
		number:   rangeNumber,
	}

	file, err := fs.OpenFile(dr.getPath(), os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = fs.MkdirAll(filepath.Dir(dr.getPath()), 0755)

			if err != nil {
				log.Println("Error creating range directory", err)
				return nil, err
			}

			file, err = fs.OpenFile(dr.getPath(), os.O_CREATE|os.O_RDWR, 0644)

			if err != nil {
				log.Println("Error creating range file", err)
				return nil, err
			}
		} else {
			log.Println("Error opening range file", err)
			return nil, err
		}
	}

	dr.file = file

	return dr, nil
}

func (dr *DataRange) Close() error {
	err := dr.file.Close()

	if err != nil {
		log.Println("Error closing data range file", err)
		return err
	}

	dr.closed = true

	return nil
}

func (dr *DataRange) Delete() error {
	err := dr.file.Close()

	if err != nil {
		log.Println("Error closing data range file", err)

		return err
	}

	err = dr.fs.Remove(dr.getPath())

	if err != nil {
		log.Println("Error removing data range file", err)

		return err
	}

	return nil
}

func (dr *DataRange) getPath() string {
	var builder strings.Builder
	builder.Grow(len(dr.path) + 10) // Preallocate memory
	builder.WriteString(dr.path)

	// Create a strings.Builder for efficient string concatenation
	var pageNumberBuilder strings.Builder
	pageNumberBuilder.Grow(15) // Preallocate memory for 10 characters

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

func (dr *DataRange) PageCount() int64 {
	if dr.closed {
		return 0
	}

	size, err := dr.Size()

	if err != nil {
		log.Println("Error getting data range file size", err)
		return 0
	}

	return size / dr.pageSize
}

func (dr *DataRange) ReadAt(p []byte, pageNumber int64) (n int, err error) {
	if dr.closed {
		return 0, os.ErrClosed
	}

	offset := file.PageRangeOffset(pageNumber, DataRangeMaxPages, dr.pageSize)

	// Read the data from the data range file
	n, err = dr.file.ReadAt(p, offset)

	if err != nil {
		if err == io.EOF {
			return n, nil
		}

		log.Println("Error reading data range file", err)

		return 0, err
	}

	return n, nil
}

func (dr *DataRange) Size() (int64, error) {
	if dr.closed {
		return 0, os.ErrClosed
	}

	stat, err := dr.file.Stat()

	if err != nil {
		log.Println("Error getting file size", err)
		return 0, err
	}

	return stat.Size(), nil
}

func (dr *DataRange) Truncate(size int64) error {
	if dr.closed {
		return os.ErrClosed
	}

	err := dr.file.Truncate(size)

	if err != nil {
		log.Println("Error truncating data range file", err)

		return err
	}

	return nil
}

func (dr *DataRange) WriteAt(p []byte, pageNumber int64) (n int, err error) {
	if dr.closed {
		return 0, os.ErrClosed
	}

	offset := file.PageRangeOffset(pageNumber, DataRangeMaxPages, dr.pageSize)

	n, err = dr.file.WriteAt(p, offset)

	if err != nil {
		log.Println("Error writing to data range file", err)
		return 0, err
	}

	return n, nil

}
