package backups

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
)

/*
The RollbackLogEntry is a data structure used to store the data of a database page
at a given point in time. Each RollbackLogEntry contains the data of a page, the
page number, the timestamp of the entry, and the version of the entry.

When serialized, the RollbackLogEntry data is stored with a header followed by a
compressed data frame. The header is 100 bytes and header contains:
| offset | size | description |
|--------|------|-----------------------------------|
| 0      | 4    | The identifier if the entry       |
| 8      | 4    | The version number of the entry   |
| 8      | 4    | The page number                   |
| 12      | 8    | The timestamp of the entry       |
| 20     | 4    | The size of the uncompressed data |
| 24     | 4    | The size of the compressed data   |
| 28     | 20   | SHA1 hash of uncompressed data    |
| 48     | 52   | Reserved for future use           |

The compressed frame is the serialized data of the page compressed using the
s2 compression algorithm.
*/
type RollbackLogEntry struct {
	Data             []byte
	PageNumber       int64
	Timestamp        int64
	SizeCompressed   int
	SizeDecompressed int
	SHA1             []byte
	Version          uint32
}

const (
	RollbackLogVersion         = 1
	RollbackLogEntryHeaderSize = 100
)

func NewRollbackLogEntry(pageNumber, timestamp int64, data []byte) *RollbackLogEntry {
	hash := sha1.New()
	hash.Write(data)
	sha1 := hash.Sum(nil)

	return &RollbackLogEntry{
		Data:       data,
		PageNumber: pageNumber,
		SHA1:       sha1,
		Timestamp:  timestamp,
		Version:    RollbackLogVersion,
	}
}

func (rle *RollbackLogEntry) Serialize(compressionBuffer *bytes.Buffer) ([]byte, error) {
	rle.SizeDecompressed = len(rle.Data)
	compressionBufferCap := compressionBuffer.Cap()
	maxEncodedLen := s2.MaxEncodedLen(rle.SizeDecompressed)

	if compressionBufferCap < maxEncodedLen {
		compressionBuffer.Grow(maxEncodedLen - compressionBufferCap + 1)
	}

	compressed := s2.Encode(compressionBuffer.Bytes()[:0], rle.Data)

	compressionBuffer.Write(compressed)

	rle.SizeCompressed = len(compressed)

	serialized := make([]byte, RollbackLogEntryHeaderSize+rle.SizeCompressed)

	// 4 bytes for the identifier
	binary.LittleEndian.PutUint32(serialized[0:4], uint32(RollbackLogEntryID))
	// 4 bytes for the version
	binary.LittleEndian.PutUint32(serialized[4:8], rle.Version)
	// 4 bytes for the page number
	binary.LittleEndian.PutUint32(serialized[8:12], uint32(rle.PageNumber))
	// 8 bytes for the timestamp
	binary.LittleEndian.PutUint64(serialized[12:20], uint64(rle.Timestamp))
	// 4 bytes for the size of the uncompressed data
	binary.LittleEndian.PutUint32(serialized[20:24], uint32(len(rle.Data)))
	// 4 bytes for the size of the compressed data
	binary.LittleEndian.PutUint32(serialized[24:28], uint32(rle.SizeCompressed))
	// 20 bytes for the SHA1 hash of the uncompressed data
	copy(serialized[28:48], []byte(rle.SHA1))
	// The remaining 52 bytes are reserved for future use and are already zero

	// Copy the compressed data to the serialized buffer
	copy(serialized[RollbackLogEntryHeaderSize:], compressed)

	return serialized, nil
}

// RollbackLogEntry are read from the file in reverse order, so we need to
// deserialize the entry from the end of the file.
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
	pageNumber := int64(binary.LittleEndian.Uint32(header[8:12]))
	// 8 bytes for the timestamp
	timestamp := int64(binary.LittleEndian.Uint64(header[12:20]))
	// 4 bytes for the size of the uncompressed data
	decompressedSize := binary.LittleEndian.Uint32(header[20:24])
	// 4 bytes for the size of the compressed data
	compressedSize := binary.LittleEndian.Uint32(header[24:28])
	// 20 bytes for the SHA1 hash of the uncompressed data
	entrySHA1 := header[28:48]

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

	hash := sha1.New()
	hash.Write((decompressed))
	calculatedSha1 := hash.Sum(nil)

	if bytes.Compare(entrySHA1, []byte(calculatedSha1)) != 0 {
		return nil, fmt.Errorf("SHA1 hash mismatch")
	}

	return &RollbackLogEntry{
		Data:             decompressed,
		PageNumber:       pageNumber,
		Timestamp:        timestamp,
		SizeCompressed:   int(compressedSize),
		SizeDecompressed: int(decompressedSize),
		Version:          version,
	}, nil
}
