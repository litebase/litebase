package storage

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// EncryptedStreamFile wraps a File with AES-256-CTR encryption
// for WAL and Page Log files. It transparently encrypts/decrypts
// 4096-byte pages using stream encryption.
type EncryptedStreamFile struct {
	file      internalStorage.File
	dataKey   []byte   // 32-byte encryption key
	keyHash   [32]byte // SHA256 hash of the encryption key
	timestamp int64    // WAL timestamp (0 for Page Logs)
	filePath  string   // File path for IV derivation
}

// NewEncryptedStreamFile creates a new encrypted stream file wrapper
// dataKey: 32-byte encryption key
// keyHash: SHA256 hash of the key (for header verification)
// timestamp: WAL timestamp (use 0 for Page Logs)
// filePath: path used for IV derivation
func NewEncryptedStreamFile(file internalStorage.File, dataKey []byte, keyHash [32]byte, timestamp int64, filePath string) (*EncryptedStreamFile, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	esf := &EncryptedStreamFile{
		file:      file,
		dataKey:   dataKey,
		keyHash:   keyHash,
		timestamp: timestamp,
		filePath:  filePath,
	}

	return esf, nil
}

// OpenEncryptedStreamFile opens an existing encrypted stream file and verifies the header
func OpenEncryptedStreamFile(file internalStorage.File, dataKey []byte, keyHash [32]byte, timestamp int64, filePath string) (*EncryptedStreamFile, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	// Read and verify header
	headerBuf := make([]byte, StreamHeaderSize)
	n, err := file.ReadAt(headerBuf, 0)

	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	if n > 0 {
		// Verify header
		header, err := ReadStreamHeader(bytes.NewReader(headerBuf[:n]))

		if err != nil {
			return nil, fmt.Errorf("invalid header: %w", err)
		}

		// Verify key hash matches
		if !bytes.Equal(header.KeyHash[:], keyHash[:]) {
			return nil, fmt.Errorf("key hash mismatch: file encrypted with different key")
		}
	}

	esf := &EncryptedStreamFile{
		file:      file,
		dataKey:   dataKey,
		keyHash:   keyHash,
		timestamp: timestamp,
		filePath:  filePath,
	}

	return esf, nil
}

// WriteHeader writes the 64-byte header to the file
func (esf *EncryptedStreamFile) WriteHeader() error {
	buf := &bytes.Buffer{}
	err := WriteStreamHeader(buf, esf.keyHash)

	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	_, err = esf.file.WriteAt(buf.Bytes(), 0)

	if err != nil {
		return fmt.Errorf("failed to write header to file: %w", err)
	}

	return nil
}

// Close closes the underlying file
func (esf *EncryptedStreamFile) Close() error {
	return esf.file.Close()
}

// Read is not supported for encrypted stream files (use ReadAt)
func (esf *EncryptedStreamFile) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("Read not supported on EncryptedStreamFile, use ReadAt")
}

// ReadAt reads and decrypts a 4096-byte page at the given offset
// The offset is automatically adjusted for the 64-byte header
func (esf *EncryptedStreamFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) != StreamPageSize {
		return 0, fmt.Errorf("ReadAt buffer must be %d bytes, got %d", StreamPageSize, len(p))
	}

	// Calculate page number from offset
	if off%StreamPageSize != 0 {
		return 0, fmt.Errorf("offset must be page-aligned (%d bytes), got %d", StreamPageSize, off)
	}

	pageNumber := uint64(off / StreamPageSize)

	// Adjust offset for header (add 64 bytes)
	adjustedOffset := off + StreamHeaderSize

	// Read encrypted page from file
	encryptedPage := make([]byte, StreamPageSize)
	n, err = esf.file.ReadAt(encryptedPage, adjustedOffset)

	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read encrypted page: %w", err)
	}

	if n == 0 {
		return 0, io.EOF
	}

	// If we read less than a full page, it might be the last page
	if n < StreamPageSize {
		// Pad with zeros for decryption
		for i := n; i < StreamPageSize; i++ {
			encryptedPage[i] = 0
		}
	}

	// Decrypt the page
	decrypted, err := DecryptPageCTR(esf.dataKey, encryptedPage, pageNumber, esf.timestamp, esf.filePath)

	if err != nil {
		return 0, fmt.Errorf("failed to decrypt page: %w", err)
	}

	// Copy decrypted data to output buffer
	copy(p, decrypted)

	return n, nil
}

// Seek is not supported for encrypted stream files
func (esf *EncryptedStreamFile) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("Seek not supported on EncryptedStreamFile")
}

// Stat returns file information from the underlying file
func (esf *EncryptedStreamFile) Stat() (fs.FileInfo, error) {
	return esf.file.Stat()
}

// Sync syncs the underlying file
func (esf *EncryptedStreamFile) Sync() error {
	return esf.file.Sync()
}

// Truncate truncates the underlying file (adjusting for header)
func (esf *EncryptedStreamFile) Truncate(size int64) error {
	// Adjust size for header
	adjustedSize := size + StreamHeaderSize

	return esf.file.Truncate(adjustedSize)
}

// Write is not supported for encrypted stream files (use WriteAt)
func (esf *EncryptedStreamFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("Write not supported on EncryptedStreamFile, use WriteAt")
}

// WriteAt encrypts and writes a 4096-byte page at the given offset
// The offset is automatically adjusted for the 64-byte header
func (esf *EncryptedStreamFile) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) != StreamPageSize {
		return 0, fmt.Errorf("WriteAt buffer must be %d bytes, got %d", StreamPageSize, len(p))
	}

	// Calculate page number from offset
	if off%StreamPageSize != 0 {
		return 0, fmt.Errorf("offset must be page-aligned (%d bytes), got %d", StreamPageSize, off)
	}

	pageNumber := uint64(off / StreamPageSize)

	// Encrypt the page
	encrypted, err := EncryptPageCTR(esf.dataKey, p, pageNumber, esf.timestamp, esf.filePath)

	if err != nil {
		return 0, fmt.Errorf("failed to encrypt page: %w", err)
	}

	// Adjust offset for header (add 64 bytes)
	adjustedOffset := off + StreamHeaderSize

	// Write encrypted page to file
	n, err = esf.file.WriteAt(encrypted, adjustedOffset)

	if err != nil {
		return 0, fmt.Errorf("failed to write encrypted page: %w", err)
	}

	return n, nil
}

// WriteTo is not supported for encrypted stream files
func (esf *EncryptedStreamFile) WriteTo(w io.Writer) (n int64, err error) {
	return 0, fmt.Errorf("WriteTo not supported on EncryptedStreamFile")
}

// WriteString is not supported for encrypted stream files
func (esf *EncryptedStreamFile) WriteString(s string) (ret int, err error) {
	return 0, fmt.Errorf("WriteString not supported on EncryptedStreamFile")
}

// UnderlyingFile returns the wrapped file (for testing/debugging)
func (esf *EncryptedStreamFile) UnderlyingFile() internalStorage.File {
	return esf.file
}
