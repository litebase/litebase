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
The PageLogEntry is a data structure used to store the data of a database page
at a given point in time. Each PageLogEntry contains the data of a page, the
page number, the timestamp of the entry, and the version of the page.

When serialized, the PageLogEntry data is stored with a header followed by a
compressed frame. The header is 100 bytes and header contains:
| offset | size | description |
|--------|------|-----------------------------------|
| 0      | 4    | The version of the page           |
| 4      | 4    | The page number of the page       |
| 8      | 8    | The timestamp of the entry        |
| 16     | 4    | The size of the uncompressed data |
| 20     | 4    | The size of the compressed data   |
| 24     | 20   | The SHA1 hash of the uncompressed data |
| 44     | 66   | Reserved for future use           |

The compressed frame is the serialized data of the page compressed using the
s2 compression algorithm.
*/
type PageLogEntry struct {
	Data             []byte
	PageNumber       uint32
	Timestamp        uint64
	SizeCompressed   int
	SizeDecompressed int
	SHA1             []byte
	Version          uint32
}

const (
	PageLogVersion    = 1
	PageLogHeaderSize = 100
)

func NewPageLogEntry(pageNumber uint32, timestamp uint64, data []byte) *PageLogEntry {
	hash := sha1.New()
	hash.Write(data)
	sha1 := hash.Sum(nil)

	return &PageLogEntry{
		Data:       data,
		PageNumber: pageNumber,
		SHA1:       sha1,
		Timestamp:  timestamp,
		Version:    PageLogVersion,
	}
}

func (p *PageLogEntry) Serialize(compressionBuffer *bytes.Buffer) ([]byte, error) {
	p.SizeDecompressed = len(p.Data)
	compressionBufferCap := compressionBuffer.Cap()
	maxEncodedLen := s2.MaxEncodedLen(p.SizeDecompressed)

	if compressionBufferCap < maxEncodedLen {
		compressionBuffer.Grow(maxEncodedLen - compressionBufferCap + 1)
	}

	compressed := s2.Encode(compressionBuffer.Bytes()[:0], p.Data)

	compressionBuffer.Write(compressed)

	p.SizeCompressed = len(compressed)

	serialized := make([]byte, PageLogHeaderSize+compressionBuffer.Len())

	// 4 bytes for the version
	binary.LittleEndian.PutUint32(serialized[0:4], uint32(p.Version))
	// 4 bytes for the page number
	binary.LittleEndian.PutUint32(serialized[4:8], uint32(p.PageNumber))
	// 8 bytes for the timestamp
	binary.LittleEndian.PutUint64(serialized[8:16], p.Timestamp)
	// 4 bytes for the size of the uncompressed data
	binary.LittleEndian.PutUint32(serialized[16:20], uint32(len(p.Data)))
	// 4 bytes for the size of the compressed data
	binary.LittleEndian.PutUint32(serialized[20:24], uint32(p.SizeCompressed))
	// 20 bytes for the SHA1 hash of the uncompressed data
	copy(serialized[24:44], []byte(p.SHA1))
	// The remaining 66 bytes are reserved for future use and are already zero

	copy(serialized[100:], compressed)

	return serialized, nil
}

func DeserializePageLogEntry(reader io.Reader) (*PageLogEntry, error) {
	header := make([]byte, 100)

	_, err := reader.Read(header)

	if err != nil {
		return nil, err
	}

	// 4 bytes for the version
	version := binary.LittleEndian.Uint32(header[0:4])
	// 4 bytes for the page number
	pageNumber := binary.LittleEndian.Uint32(header[4:8])
	// 8 bytes for the timestamp
	timestamp := binary.LittleEndian.Uint64(header[8:16])
	// 4 bytes for the size of the uncompressed data
	decompressedSize := binary.LittleEndian.Uint32(header[16:20])
	// 4 bytes for the size of the compressed data
	compressedSize := binary.LittleEndian.Uint32(header[20:24])
	// 20 bytes for the SHA1 hash of the uncompressed data
	entrySHA1 := header[24:44]

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

	return &PageLogEntry{
		Data:             decompressed,
		PageNumber:       pageNumber,
		Timestamp:        timestamp,
		SizeCompressed:   int(compressedSize),
		SizeDecompressed: int(decompressedSize),
		Version:          version,
	}, nil
}
