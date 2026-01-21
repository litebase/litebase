package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

const (
	// AuthenticatedHeaderMagic is the magic bytes for authenticated encrypted files
	AuthenticatedHeaderMagic = "LENC"
	// AuthenticatedHeaderVersion is the current version
	AuthenticatedHeaderVersion = 1
	// MaxPagesPerFile is the maximum number of pages that can be stored in a single file
	MaxPagesPerFile = 4096
	// AuthenticatedHeaderSize is the total header size: 64B fixed + 32KB offset table
	AuthenticatedHeaderSize = 64 + (MaxPagesPerFile * 8)
)

// AuthenticatedHeader represents the header for authenticated encrypted files
type AuthenticatedHeader struct {
	Magic       [4]byte                // "LENC"
	Version     byte                   // Format version (currently 1)
	KeyHash     [32]byte               // SHA256 hash of the data encryption key
	PageCount   uint64                 // Number of pages currently stored
	Reserved    [19]byte               // Reserved for future use
	PageOffsets [MaxPagesPerFile]int64 // Offset to each encrypted page
}

// EncryptedAuthenticatedFile wraps a File with AES-256-GCM encryption and compression
// for Data Range files. Each page is compressed, encrypted, and stored at variable offsets.
type EncryptedAuthenticatedFile struct {
	file     internalStorage.File
	dataKey  []byte   // 32-byte encryption key
	keyHash  [32]byte // SHA256 hash of the encryption key
	filePath string   // File path for key derivation
	header   *AuthenticatedHeader
	mutex    sync.RWMutex // Protects header updates
}

// NewEncryptedAuthenticatedFile creates a new authenticated encrypted file wrapper
func NewEncryptedAuthenticatedFile(file internalStorage.File, dataKey []byte, keyHash [32]byte, filePath string) (*EncryptedAuthenticatedFile, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	header := &AuthenticatedHeader{
		Version:   AuthenticatedHeaderVersion,
		KeyHash:   keyHash,
		PageCount: 0,
	}
	copy(header.Magic[:], []byte(AuthenticatedHeaderMagic))

	eaf := &EncryptedAuthenticatedFile{
		file:     file,
		dataKey:  dataKey,
		keyHash:  keyHash,
		filePath: filePath,
		header:   header,
	}

	return eaf, nil
}

// OpenEncryptedAuthenticatedFile opens an existing authenticated encrypted file
func OpenEncryptedAuthenticatedFile(file internalStorage.File, dataKey []byte, keyHash [32]byte, filePath string) (*EncryptedAuthenticatedFile, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	eaf := &EncryptedAuthenticatedFile{
		file:     file,
		dataKey:  dataKey,
		keyHash:  keyHash,
		filePath: filePath,
	}

	// Read and verify header
	err := eaf.readHeader()

	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	return eaf, nil
}

// WriteHeader writes the header to the file
func (eaf *EncryptedAuthenticatedFile) WriteHeader() error {
	eaf.mutex.RLock()
	defer eaf.mutex.RUnlock()

	buf := &bytes.Buffer{}

	// Write magic
	if _, err := buf.Write(eaf.header.Magic[:]); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}

	// Write version
	if err := buf.WriteByte(eaf.header.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write key hash
	if _, err := buf.Write(eaf.header.KeyHash[:]); err != nil {
		return fmt.Errorf("failed to write key hash: %w", err)
	}

	// Write page count
	if err := binary.Write(buf, binary.BigEndian, eaf.header.PageCount); err != nil {
		return fmt.Errorf("failed to write page count: %w", err)
	}

	// Write reserved bytes
	if _, err := buf.Write(eaf.header.Reserved[:]); err != nil {
		return fmt.Errorf("failed to write reserved: %w", err)
	}

	// Write page offsets
	for i := range MaxPagesPerFile {
		if err := binary.Write(buf, binary.BigEndian, eaf.header.PageOffsets[i]); err != nil {
			return fmt.Errorf("failed to write page offset %d: %w", i, err)
		}
	}

	// Write header to file
	_, err := eaf.file.WriteAt(buf.Bytes(), 0)

	if err != nil {
		return fmt.Errorf("failed to write header to file: %w", err)
	}

	return nil
}

// readHeader reads and validates the header from the file
func (eaf *EncryptedAuthenticatedFile) readHeader() error {
	headerBuf := make([]byte, AuthenticatedHeaderSize)
	n, err := eaf.file.ReadAt(headerBuf, 0)

	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if n < AuthenticatedHeaderSize {
		return fmt.Errorf("header too short: expected %d bytes, got %d", AuthenticatedHeaderSize, n)
	}

	buf := bytes.NewReader(headerBuf)

	header := &AuthenticatedHeader{}

	// Read magic
	if _, err := io.ReadFull(buf, header.Magic[:]); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}

	if string(header.Magic[:]) != AuthenticatedHeaderMagic {
		return fmt.Errorf("invalid magic: expected %q, got %q", AuthenticatedHeaderMagic, string(header.Magic[:]))
	}

	// Read version
	version, err := buf.ReadByte()

	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	header.Version = version

	if header.Version != AuthenticatedHeaderVersion {
		return fmt.Errorf("unsupported version: expected %d, got %d", AuthenticatedHeaderVersion, header.Version)
	}

	// Read key hash
	if _, err := io.ReadFull(buf, header.KeyHash[:]); err != nil {
		return fmt.Errorf("failed to read key hash: %w", err)
	}

	// Verify key hash
	if !bytes.Equal(header.KeyHash[:], eaf.keyHash[:]) {
		return fmt.Errorf("key hash mismatch: file encrypted with different key")
	}

	// Read page count
	if err := binary.Read(buf, binary.BigEndian, &header.PageCount); err != nil {
		return fmt.Errorf("failed to read page count: %w", err)
	}

	// Read reserved bytes
	if _, err := io.ReadFull(buf, header.Reserved[:]); err != nil {
		return fmt.Errorf("failed to read reserved: %w", err)
	}

	// Read page offsets
	for i := range MaxPagesPerFile {
		if err := binary.Read(buf, binary.BigEndian, &header.PageOffsets[i]); err != nil {
			return fmt.Errorf("failed to read page offset %d: %w", i, err)
		}
	}

	eaf.mutex.Lock()
	eaf.header = header
	eaf.mutex.Unlock()

	return nil
}

// Close closes the underlying file
func (eaf *EncryptedAuthenticatedFile) Close() error {
	return eaf.file.Close()
}

// Read is not supported (use ReadAt)
func (eaf *EncryptedAuthenticatedFile) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("Read not supported on EncryptedAuthenticatedFile, use ReadAt")
}

// ReadAt reads and decrypts a page at the given page number
func (eaf *EncryptedAuthenticatedFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) != AuthenticatedPageSize {
		return 0, fmt.Errorf("ReadAt buffer must be %d bytes, got %d", AuthenticatedPageSize, len(p))
	}

	// Calculate page number from offset
	if off%AuthenticatedPageSize != 0 {
		return 0, fmt.Errorf("offset must be page-aligned (%d bytes), got %d", AuthenticatedPageSize, off)
	}

	pageNumber := uint64(off / AuthenticatedPageSize)

	eaf.mutex.RLock()
	defer eaf.mutex.RUnlock()

	// Check if page exists
	if pageNumber >= eaf.header.PageCount {
		return 0, io.EOF
	}

	// Get page offset from header
	pageOffset := eaf.header.PageOffsets[pageNumber]

	if pageOffset == 0 {
		return 0, fmt.Errorf("invalid page offset for page %d", pageNumber)
	}

	// Determine page size (read until next page offset or EOF)
	var pageSize int64

	if pageNumber+1 < eaf.header.PageCount {
		nextOffset := eaf.header.PageOffsets[pageNumber+1]
		pageSize = nextOffset - pageOffset
	} else {
		// Last page - read to EOF
		fileInfo, err := eaf.file.Stat()

		if err != nil {
			return 0, fmt.Errorf("failed to stat file: %w", err)
		}

		pageSize = fileInfo.Size() - pageOffset
	}

	if pageSize <= 0 {
		return 0, fmt.Errorf("invalid page size %d for page %d", pageSize, pageNumber)
	}

	// Read encrypted page data
	encryptedData := make([]byte, pageSize)
	n, err = eaf.file.ReadAt(encryptedData, pageOffset)

	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read encrypted page: %w", err)
	}

	// Decrypt and decompress the page
	decrypted, err := DecryptPageGCM(eaf.dataKey, encryptedData[:n], pageNumber, eaf.filePath)

	if err != nil {
		return 0, fmt.Errorf("failed to decrypt page: %w", err)
	}

	// Copy to output buffer
	copy(p, decrypted)

	return len(decrypted), nil
}

// Seek is not supported
func (eaf *EncryptedAuthenticatedFile) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("Seek not supported on EncryptedAuthenticatedFile")
}

// Stat returns file information
func (eaf *EncryptedAuthenticatedFile) Stat() (fs.FileInfo, error) {
	return eaf.file.Stat()
}

// Sync syncs the underlying file
func (eaf *EncryptedAuthenticatedFile) Sync() error {
	return eaf.file.Sync()
}

// Truncate is not supported for authenticated files
func (eaf *EncryptedAuthenticatedFile) Truncate(size int64) error {
	return fmt.Errorf("Truncate not supported on EncryptedAuthenticatedFile")
}

// Write is not supported (use WriteAt)
func (eaf *EncryptedAuthenticatedFile) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("Write not supported on EncryptedAuthenticatedFile, use WriteAt")
}

// WriteAt compresses, encrypts, and appends a page to the file
func (eaf *EncryptedAuthenticatedFile) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) != AuthenticatedPageSize {
		return 0, fmt.Errorf("WriteAt buffer must be %d bytes, got %d", AuthenticatedPageSize, len(p))
	}

	// Calculate page number from offset
	if off%AuthenticatedPageSize != 0 {
		return 0, fmt.Errorf("offset must be page-aligned (%d bytes), got %d", AuthenticatedPageSize, off)
	}

	pageNumber := uint64(off / AuthenticatedPageSize)

	if pageNumber >= MaxPagesPerFile {
		return 0, fmt.Errorf("page number %d exceeds maximum %d", pageNumber, MaxPagesPerFile)
	}

	// Encrypt and compress the page
	encrypted, err := EncryptPageGCM(eaf.dataKey, p, pageNumber, eaf.filePath)

	if err != nil {
		return 0, fmt.Errorf("failed to encrypt page: %w", err)
	}

	eaf.mutex.Lock()
	defer eaf.mutex.Unlock()

	// Determine write offset (append to end of file)
	var writeOffset int64

	if eaf.header.PageCount > 0 {
		// Get offset of last written page
		lastPageIdx := eaf.header.PageCount - 1
		lastPageOffset := eaf.header.PageOffsets[lastPageIdx]

		// Determine size of last page
		fileInfo, err := eaf.file.Stat()

		if err != nil {
			return 0, fmt.Errorf("failed to stat file: %w", err)
		}

		writeOffset = fileInfo.Size()

		// Validate last page offset
		if lastPageOffset < AuthenticatedHeaderSize || lastPageOffset >= writeOffset {
			return 0, fmt.Errorf("invalid last page offset: %d", lastPageOffset)
		}
	} else {
		// First page - write after header
		writeOffset = AuthenticatedHeaderSize
	}

	// Write encrypted data to file
	n, err = eaf.file.WriteAt(encrypted, writeOffset)

	if err != nil {
		return 0, fmt.Errorf("failed to write encrypted page: %w", err)
	}

	// Update header with new page offset
	eaf.header.PageOffsets[pageNumber] = writeOffset

	// Update page count if necessary
	if pageNumber >= eaf.header.PageCount {
		eaf.header.PageCount = pageNumber + 1
	}

	// Write updated header
	if err := eaf.writeHeaderUnlocked(); err != nil {
		return 0, fmt.Errorf("failed to update header: %w", err)
	}

	return len(p), nil
}

// writeHeaderUnlocked writes the header without locking (caller must hold lock)
func (eaf *EncryptedAuthenticatedFile) writeHeaderUnlocked() error {
	buf := &bytes.Buffer{}

	// Write magic
	if _, err := buf.Write(eaf.header.Magic[:]); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}

	// Write version
	if err := buf.WriteByte(eaf.header.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write key hash
	if _, err := buf.Write(eaf.header.KeyHash[:]); err != nil {
		return fmt.Errorf("failed to write key hash: %w", err)
	}

	// Write page count
	if err := binary.Write(buf, binary.BigEndian, eaf.header.PageCount); err != nil {
		return fmt.Errorf("failed to write page count: %w", err)
	}

	// Write reserved bytes
	if _, err := buf.Write(eaf.header.Reserved[:]); err != nil {
		return fmt.Errorf("failed to write reserved: %w", err)
	}

	// Write page offsets
	for i := range MaxPagesPerFile {
		if err := binary.Write(buf, binary.BigEndian, eaf.header.PageOffsets[i]); err != nil {
			return fmt.Errorf("failed to write page offset %d: %w", i, err)
		}
	}

	// Write header to file
	_, err := eaf.file.WriteAt(buf.Bytes(), 0)

	if err != nil {
		return fmt.Errorf("failed to write header to file: %w", err)
	}

	return nil
}

// WriteTo is not supported
func (eaf *EncryptedAuthenticatedFile) WriteTo(w io.Writer) (n int64, err error) {
	return 0, fmt.Errorf("WriteTo not supported on EncryptedAuthenticatedFile")
}

// WriteString is not supported
func (eaf *EncryptedAuthenticatedFile) WriteString(s string) (ret int, err error) {
	return 0, fmt.Errorf("WriteString not supported on EncryptedAuthenticatedFile")
}

// UnderlyingFile returns the wrapped file (for testing/debugging)
func (eaf *EncryptedAuthenticatedFile) UnderlyingFile() internalStorage.File {
	return eaf.file
}

// PageCount returns the current number of pages
func (eaf *EncryptedAuthenticatedFile) PageCount() uint64 {
	eaf.mutex.RLock()
	defer eaf.mutex.RUnlock()

	return eaf.header.PageCount
}
