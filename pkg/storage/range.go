package storage

import (
	"fmt"
	"io"
	"log/slog"
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
	dataKey    []byte // Optional: encryption key (32 bytes)
	encrypted  bool   // Whether this Range is encrypted
	file       internalStorage.File
	fs         *FileSystem
	keyHash    [32]byte // Optional: SHA256 hash of encryption key
	pageSize   int64
	number     int64
}

// NewRange creates a new range for the specified path.
func NewRange(databaseId, branchId string, fs *FileSystem, rangeNumber int64, pageSize int64) (*Range, error) {
	dr := &Range{
		branchId:   branchId,
		databaseId: databaseId,
		encrypted:  false,
		fs:         fs,
		pageSize:   pageSize,
		number:     rangeNumber,
	}

tryOpen:
	file, err := fs.OpenFile(dr.Path(), os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = fs.MkdirAll(filepath.Dir(dr.Path()), 0750)

			if err != nil {
				slog.Error("Error creating range directory", "error", err)

				return nil, err
			}

			goto tryOpen
		} else {
			slog.Error("Error opening range file", "error", err)
			return nil, err
		}
	}

	dr.file = file

	return dr, nil
}

// NewEncryptedRange creates a new encrypted range for the specified path.
// dataKey must be exactly 32 bytes.
func NewEncryptedRange(databaseId, branchId string, fs *FileSystem, rangeNumber int64, pageSize int64, dataKey []byte, keyHash [32]byte) (*Range, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be exactly 32 bytes, got %d", len(dataKey))
	}

	dr := &Range{
		branchId:   branchId,
		databaseId: databaseId,
		dataKey:    dataKey,
		encrypted:  true,
		fs:         fs,
		keyHash:    keyHash,
		pageSize:   pageSize,
		number:     rangeNumber,
	}

tryOpen:
	file, err := fs.OpenFile(dr.Path(), os.O_CREATE|os.O_RDWR, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = fs.MkdirAll(filepath.Dir(dr.Path()), 0750)

			if err != nil {
				slog.Error("Error creating range directory", "error", err)

				return nil, err
			}

			goto tryOpen
		} else {
			slog.Error("Error opening range file", "error", err)
			return nil, err
		}
	}

	// Wrap with encrypted file
	fileInfo, err := file.Stat()

	if err != nil {
		if err := file.Close(); err != nil {
			slog.Error("failed to close file after stat error:", "error", err)
		}

		return nil, fmt.Errorf("failed to stat range file: %w", err)
	}

	var encryptedFile internalStorage.File

	// Use portable encryption path for branch copying support
	// Format: database/{databaseId}/range/{rangeNumber}
	// This allows encrypted range files to be copied between branches
	encryptionPath := fmt.Sprintf("database/%s/range/%d", databaseId, rangeNumber)

	if fileInfo.Size() == 0 {
		// New file - create encrypted stream wrapper
		encryptedFile, err = NewEncryptedStreamFile(file, dataKey, keyHash, 0, encryptionPath)

		if err != nil {
			if err := file.Close(); err != nil {
				slog.Error("failed to close file after error creating encrypted range file:", "error", err)
			}

			return nil, fmt.Errorf("failed to create encrypted range file: %w", err)
		}

		// Write the header
		err = encryptedFile.(*EncryptedStreamFile).WriteHeader()

		if err != nil {
			if err := file.Close(); err != nil {
				slog.Error("failed to close file after error writing encrypted range header:", "error", err)
			}

			return nil, fmt.Errorf("failed to write encrypted range header: %w", err)
		}
	} else {
		// Existing file - open encrypted stream wrapper
		encryptedFile, err = OpenEncryptedStreamFile(file, dataKey, keyHash, 0, encryptionPath)

		if err != nil {
			if err := file.Close(); err != nil {
				slog.Error("failed to close file after error opening encrypted range file:", "error", err)
			}

			return nil, fmt.Errorf("failed to open encrypted range file: %w", err)
		}
	}

	dr.file = encryptedFile

	return dr, nil
}

func (dr *Range) Close() error {
	if dr.closed {
		return nil
	}

	dr.closed = true

	err := dr.file.Close()

	if err != nil {
		return fmt.Errorf("failed to close range file: %w", err)
	}

	return nil
}

func (dr *Range) Delete() error {
	err := dr.fs.Remove(dr.Path())

	if err != nil {
		slog.Debug("Error removing range file", "error", err)

		return err
	}

	return nil
}

// The unique identifier for the range file.
func (dr *Range) ID() string {
	return fmt.Sprintf("%010d", dr.number)
}

// The number of pages in the range file.
func (dr *Range) PageCount() int64 {
	if dr.closed {
		return 0
	}

	size, err := dr.Size()

	if err != nil {
		slog.Error("Error getting range file size", "error", err)
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

		slog.Error("Error reading range file", "error", err)

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
		slog.Error("Error getting file size", "error", err)
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
		slog.Error("Error truncating range file", "error", err)

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
		slog.Error("Error writing to range file", "error", err)
		return 0, err
	}

	return n, nil
}
