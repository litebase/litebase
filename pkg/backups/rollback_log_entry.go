package backups

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/litebase/litebase/internal/utils"
)

/*
The RollbackLogEntry is a data structure used to store the data of a database page
at a given point in time. Each RollbackLogEntry contains the data of a page, the
page number, the timestamp of the entry, and the version of the entry.

When serialized, the RollbackLogEntry data is stored with a header followed by a
compressed data frame. The header is 100 bytes and header contains:
| offset | size | description                       |
|--------|------|-----------------------------------|
| 0      | 4    | The identifier if the entry       |
| 8      | 4    | The version number of the entry   |
| 8      | 4    | The page number                   |
| 12      | 8    | The timestamp of the entry       |
| 20     | 4    | The size of the uncompressed data |
| 24     | 4    | The size of the compressed data   |
| 28     | 32   | SHA256 hash of uncompressed data  |
| 60     | 40   | Reserved for future use           |

The compressed frame is the serialized data of the page compressed using the
s2 compression algorithm.
*/
type RollbackLogEntry struct {
	Data             []byte
	PageNumber       int64
	Timestamp        int64
	SizeCompressed   uint32
	SizeDecompressed uint32
	SHA256           []byte
	Version          uint32
}

const (
	RollbackLogVersion         = 1
	RollbackLogEntryHeaderSize = 100
)

// Create a new RollbackLogEntry with the given parameters.
func NewRollbackLogEntry(pageNumber int64, timestamp int64, data []byte) *RollbackLogEntry {
	sha256 := sha256.Sum256(data)

	return &RollbackLogEntry{
		Data:       data,
		PageNumber: pageNumber,
		SHA256:     sha256[:],
		Timestamp:  timestamp,
		Version:    RollbackLogVersion,
	}
}

// Serialize the RollbackLogEntry into a byte slice.
func (rle *RollbackLogEntry) Serialize(compressionBuffer *bytes.Buffer) ([]byte, error) {
	var err error

	rle.SizeDecompressed, err = utils.SafeIntToUint32(len(rle.Data))

	if err != nil {
		return nil, err
	}
	compressionBufferCap := compressionBuffer.Cap()
	maxEncodedLen := s2.MaxEncodedLen(int(rle.SizeDecompressed))

	if compressionBufferCap < maxEncodedLen {
		compressionBuffer.Grow(maxEncodedLen - compressionBufferCap + 1)
	}

	compressed := s2.Encode(compressionBuffer.Bytes()[:0], rle.Data)

	compressionBuffer.Write(compressed)

	rle.SizeCompressed, err = utils.SafeIntToUint32(len(compressed))

	if err != nil {
		return nil, err
	}

	serialized := make([]byte, RollbackLogEntryHeaderSize+rle.SizeCompressed)

	// 4 bytes for the identifier
	binary.LittleEndian.PutUint32(serialized[0:4], uint32(RollbackLogEntryID))
	// 4 bytes for the version
	binary.LittleEndian.PutUint32(serialized[4:8], rle.Version)

	uint32PageNumber, err := utils.SafeInt64ToUint32(rle.PageNumber)

	if err != nil {
		return nil, err
	}

	// 4 bytes for the page number
	binary.LittleEndian.PutUint32(serialized[8:12], uint32PageNumber)

	uint64Timestamp, err := utils.SafeInt64ToUint64(rle.Timestamp)

	if err != nil {
		return nil, err
	}

	// 8 bytes for the timestamp
	binary.LittleEndian.PutUint64(serialized[12:20], uint64Timestamp)

	// 4 bytes for the size of the uncompressed data
	binary.LittleEndian.PutUint32(serialized[20:24], rle.SizeDecompressed)
	// 4 bytes for the size of the compressed data
	binary.LittleEndian.PutUint32(serialized[24:28], rle.SizeCompressed)
	// 32 bytes for the SHA256 hash of the uncompressed data
	copy(serialized[28:60], rle.SHA256)
	// The remaining 40 bytes are reserved for future use and are already zero

	// Copy the compressed data to the serialized buffer
	copy(serialized[RollbackLogEntryHeaderSize:], compressed)

	return serialized, nil
}

// Deserialize  a RollbackLogEntry from a reader.
func DeserializeRollbackLogEntry(reader io.ReadSeeker) (*RollbackLogEntry, error) {
	header := make([]byte, RollbackLogEntryHeaderSize)

	_, err := reader.Read(header)

	if err != nil {
		return nil, err
	}

	// 4 bytes for the identifier
	id := RollbackLogIdentifier(binary.LittleEndian.Uint32(header[0:4]))

	if id != RollbackLogEntryID {
		return nil, fmt.Errorf("this data is not a RollbackLogEntry expected: %d got: %d", RollbackLogEntryID, id)
	}

	// 4 bytes for the version
	version := binary.LittleEndian.Uint32(header[4:8])

	// 4 bytes for the page number
	pageNumber, err := utils.SafeUint32ToInt64(binary.LittleEndian.Uint32(header[8:12]))

	if err != nil {
		return nil, err
	}

	// 8 bytes for the timestamp
	timestamp, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(header[12:20]))

	if err != nil {
		return nil, err
	}

	// 4 bytes for the size of the uncompressed data
	decompressedSize := binary.LittleEndian.Uint32(header[20:24])
	// 4 bytes for the size of the compressed data
	compressedSize := binary.LittleEndian.Uint32(header[24:28])
	// 32 bytes for the SHA256 hash of the uncompressed data
	entrySHA256 := header[28:60]

	// Read the compressed frame
	compressed := make([]byte, compressedSize)

	_, err = reader.Read(compressed)

	if err != nil {
		return nil, err
	}

	decompressed, err := s2.Decode(nil, compressed)

	if err != nil {
		return nil, err
	}

	calculatedSha256 := sha256.Sum256(decompressed)

	if !bytes.Equal(entrySHA256, calculatedSha256[:]) {
		return nil, fmt.Errorf("SHA256 hash mismatch")
	}

	return &RollbackLogEntry{
		Data:             decompressed,
		PageNumber:       pageNumber,
		Timestamp:        timestamp,
		SizeCompressed:   compressedSize,
		SizeDecompressed: decompressedSize,
		Version:          version,
	}, nil
}
