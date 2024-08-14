package storage

import (
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"log"
	"os"
	"strconv"
	"strings"
)

/*
A data range represents a subset of the data in a database. It is used to split
the database into smaller files to allow the database to scale to larger sizes
that typically would not be possible with a single file.

*/

const (
	DataRangeHeaderSize          int64 = 100
	DataRangeVersion             int32 = 1
	DataRangePageSize            int32 = 65536
	DataRangeMaxPages            int64 = 1024
	DataRangePageBlockPartitions int32 = 16
	DataRangePageBlockSize       int32 = DataRangePageSize / DataRangePageBlockPartitions
)

type DataRange struct {
	file   internalStorage.File
	lfs    *LocalDatabaseFileSystem
	path   string
	number int64
}

// NewDataRange creates a new data range for the specified path.
func NewDataRange(lfs *LocalDatabaseFileSystem, path string, rangeNumber int64) (*DataRange, error) {

	dr := &DataRange{
		lfs:    lfs,
		path:   path,
		number: rangeNumber,
	}
	file, err := lfs.fileSystem.OpenFile(dr.getPath(lfs, path, rangeNumber), os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			file, err = lfs.fileSystem.OpenFile(dr.getPath(lfs, path, rangeNumber), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

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
	return dr.file.Close()
}

func (dr *DataRange) getPath(lfs *LocalDatabaseFileSystem, path string, pageNumber int64) string {
	directory := strings.ReplaceAll(path, ".db", "")
	rangeNumber := file.PageRange(pageNumber, DataRangeMaxPages)

	var builder strings.Builder
	builder.Grow(len(lfs.path) + len(directory) + 10) // Preallocate memory
	builder.WriteString(lfs.path)
	builder.WriteString("/")
	builder.WriteString(directory)
	builder.WriteString("/")

	// Create a strings.Builder for efficient string concatenation
	var pageNumberBuilder strings.Builder
	pageNumberBuilder.Grow(15) // Preallocate memory for 10 characters

	// Convert rangeNumber to a zero-padded 10-digit string
	rangeStr := strconv.FormatInt(rangeNumber, 10)
	padding := 10 - len(rangeStr)

	for i := 0; i < padding; i++ {
		pageNumberBuilder.WriteByte('0')
	}

	pageNumberBuilder.WriteString(rangeStr)

	builder.WriteString(pageNumberBuilder.String())

	return builder.String()
}

func (dr *DataRange) ReadAt(p []byte, pageNumber int64) (n int, err error) {
	offset := file.PageRangeOffset(pageNumber, DataRangeMaxPages, dr.lfs.pageSize)

	// Read the data from the data range file
	n, err = dr.file.ReadAt(p, offset)

	if err != nil {
		log.Println("Error reading data range file", err)
		return 0, err
	}

	return n, nil
}

func (dr *DataRange) WriteAt(p []byte, pageNumber int64) (n int, err error) {
	log.Println("Writing to data range", pageNumber, len(p))
	// Append the data to the data range file
	_, err = dr.file.Seek(0, io.SeekEnd)

	if err != nil {
		log.Println("Error seeking to end of data range file", err)
		return 0, err
	}

	n, err = dr.file.Write(p)

	if err != nil {
		log.Println("Error writing to data range file", err)
		return 0, err
	}

	// Write the data to the data range file

	return n, nil

}
