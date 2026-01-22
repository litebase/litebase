package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	// StreamHeaderMagic is the magic bytes for stream-encrypted files
	StreamHeaderMagic = "LSTR"
	// StreamHeaderVersion is the current version of the stream encryption format
	StreamHeaderVersion = 1
	// StreamHeaderSize is the fixed size of the stream encryption header (64 bytes)
	StreamHeaderSize = 64
	// StreamPageSize is the fixed size of encrypted pages (4096 bytes)
	StreamPageSize = 4096
)

// StreamHeader represents the fixed 64-byte header for stream-encrypted files
type StreamHeader struct {
	Magic    [4]byte  // "LSTR"
	Version  byte     // Format version (currently 1)
	KeyHash  [32]byte // SHA256 hash of the data encryption key
	Reserved [27]byte // Reserved for future use
}

// derivePageIV derives a unique IV for a page using HKDF
// Input: dataKey (32 bytes), pageNumber, timestamp, filePath
// Output: 16-byte IV for AES-CTR
func derivePageIV(dataKey []byte, pageNumber uint64, timestamp int64, filePath string) ([]byte, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	// Create info from pageNumber || timestamp || SHA256(filePath)
	filePathHash := sha256.Sum256([]byte(filePath))

	info := make([]byte, 8+8+32) // pageNumber + timestamp + filePathHash
	binary.BigEndian.PutUint64(info[0:8], pageNumber)
	binary.BigEndian.PutUint64(info[8:16], uint64(timestamp))
	copy(info[16:48], filePathHash[:])

	// Derive 16-byte IV using HKDF-SHA256
	hkdfReader := hkdf.New(sha256.New, dataKey, nil, info)
	iv := make([]byte, aes.BlockSize) // 16 bytes for AES

	if _, err := io.ReadFull(hkdfReader, iv); err != nil {
		return nil, fmt.Errorf("failed to derive IV: %w", err)
	}

	return iv, nil
}

// WriteStreamHeader writes the 64-byte fixed header to the writer
func WriteStreamHeader(w io.Writer, keyHash [32]byte) error {
	header := StreamHeader{
		Version: StreamHeaderVersion,
		KeyHash: keyHash,
	}

	copy(header.Magic[:], []byte(StreamHeaderMagic))

	// Write magic
	if _, err := w.Write(header.Magic[:]); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}

	// Write version
	if _, err := w.Write([]byte{header.Version}); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write key hash
	if _, err := w.Write(header.KeyHash[:]); err != nil {
		return fmt.Errorf("failed to write key hash: %w", err)
	}

	// Write reserved bytes
	if _, err := w.Write(header.Reserved[:]); err != nil {
		return fmt.Errorf("failed to write reserved bytes: %w", err)
	}

	return nil
}

// ReadStreamHeader reads and validates the 64-byte fixed header from the reader
func ReadStreamHeader(r io.Reader) (*StreamHeader, error) {
	header := &StreamHeader{}

	// Read magic
	if _, err := io.ReadFull(r, header.Magic[:]); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}

	if string(header.Magic[:]) != StreamHeaderMagic {
		return nil, fmt.Errorf("invalid magic bytes: expected %q, got %q", StreamHeaderMagic, string(header.Magic[:]))
	}

	// Read version
	versionBuf := make([]byte, 1)

	if _, err := io.ReadFull(r, versionBuf); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}

	header.Version = versionBuf[0]

	if header.Version != StreamHeaderVersion {
		return nil, fmt.Errorf("unsupported version: expected %d, got %d", StreamHeaderVersion, header.Version)
	}

	// Read key hash
	if _, err := io.ReadFull(r, header.KeyHash[:]); err != nil {
		return nil, fmt.Errorf("failed to read key hash: %w", err)
	}

	// Read reserved bytes
	if _, err := io.ReadFull(r, header.Reserved[:]); err != nil {
		return nil, fmt.Errorf("failed to read reserved bytes: %w", err)
	}

	return header, nil
}

// EncryptPageCTR encrypts a 4096-byte page using AES-256-CTR
// Input: dataKey (32 bytes), pageData (4096 bytes), pageNumber, timestamp, filePath
// Output: encrypted data (4096 bytes)
func EncryptPageCTR(dataKey []byte, pageData []byte, pageNumber uint64, timestamp int64, filePath string) ([]byte, error) {
	if len(pageData) != StreamPageSize {
		return nil, fmt.Errorf("pageData must be %d bytes, got %d", StreamPageSize, len(pageData))
	}

	// Derive unique IV for this page
	iv, err := derivePageIV(dataKey, pageNumber, timestamp, filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to derive IV: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(dataKey)

	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create CTR stream
	stream := cipher.NewCTR(block, iv)

	// Encrypt the page
	encrypted := make([]byte, StreamPageSize)
	stream.XORKeyStream(encrypted, pageData)

	return encrypted, nil
}

// DecryptPageCTR decrypts a 4096-byte page using AES-256-CTR
// Input: dataKey (32 bytes), encryptedData (4096 bytes), pageNumber, timestamp, filePath
// Output: decrypted data (4096 bytes)
func DecryptPageCTR(dataKey []byte, encryptedData []byte, pageNumber uint64, timestamp int64, filePath string) ([]byte, error) {
	if len(encryptedData) != StreamPageSize {
		return nil, fmt.Errorf("encryptedData must be %d bytes, got %d", StreamPageSize, len(encryptedData))
	}

	// Derive the same IV used for encryption
	iv, err := derivePageIV(dataKey, pageNumber, timestamp, filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to derive IV: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(dataKey)

	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create CTR stream (CTR encryption/decryption are the same operation)
	stream := cipher.NewCTR(block, iv)

	// Decrypt the page
	decrypted := make([]byte, StreamPageSize)
	stream.XORKeyStream(decrypted, encryptedData)

	return decrypted, nil
}

// EncryptStreamCTR encrypts arbitrary-length data using AES-256-CTR at a specific file offset
// This allows encrypting data of any size at any position in the stream
func EncryptStreamCTR(dataKey []byte, data []byte, fileOffset int64, timestamp int64, filePath string) ([]byte, error) {
	if len(data) == 0 {
		return []byte{}, nil
	}

	// Derive IV based on the file offset (treating offset as "page number" in 16-byte blocks)
	blockNumber := uint64(fileOffset / 16)
	blockOffset := fileOffset % 16

	iv, err := derivePageIV(dataKey, blockNumber, timestamp, filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to derive IV: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(dataKey)

	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create CTR stream
	stream := cipher.NewCTR(block, iv)

	// If we're not starting at a block boundary, we need to sync the stream
	if blockOffset != 0 {
		// Discard bytes to sync the CTR stream to the correct position
		discard := make([]byte, blockOffset)
		stream.XORKeyStream(discard, discard)
	}

	// Encrypt the data
	encrypted := make([]byte, len(data))
	stream.XORKeyStream(encrypted, data)

	return encrypted, nil
}

// DecryptStreamCTR decrypts arbitrary-length data using AES-256-CTR at a specific file offset
// This is the same as EncryptStreamCTR because CTR mode encryption and decryption are identical
func DecryptStreamCTR(dataKey []byte, encryptedData []byte, fileOffset int64, timestamp int64, filePath string) ([]byte, error) {
	return EncryptStreamCTR(dataKey, encryptedData, fileOffset, timestamp, filePath)
}
