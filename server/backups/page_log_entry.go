package backups

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
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
zstd compression algorithm.
*/
type PageLogEntry struct {
	Data             []byte
	PageNumber       uint32
	Timestamp        uint64
	SizeCompressed   int
	SizeDecompressed int
	SHA1             string
	Version          uint32
}

const version = 1

var encoder, _ = zstd.NewWriter(nil)
var decoder, _ = zstd.NewReader(nil)

func NewPageLogEntry(pageNumber uint32, timestamp uint64, data []byte) *PageLogEntry {
	hash := sha1.New()
	hash.Write(data)
	sha1 := fmt.Sprintf("%x", hash.Sum(nil))

	return &PageLogEntry{
		Data:       data,
		PageNumber: pageNumber,
		SHA1:       sha1,
		Timestamp:  timestamp,
		Version:    version,
	}
}

func (p *PageLogEntry) Serialize() ([]byte, error) {
	compressed := encoder.EncodeAll(p.Data, nil)

	serialized := []byte{}
	header := make([]byte, 100) // Create a byte slice of size 100

	// 4 bytes for the version
	binary.LittleEndian.PutUint32(header[0:4], uint32(p.Version))
	// 4 bytes for the page number
	binary.LittleEndian.PutUint32(header[4:8], uint32(p.PageNumber))
	// 8 bytes for the timestamp
	binary.LittleEndian.PutUint64(header[8:16], p.Timestamp)
	// 4 bytes for the size of the uncompressed data
	binary.LittleEndian.PutUint32(header[16:20], uint32(len(p.Data)))
	// 4 bytes for the size of the compressed data
	binary.LittleEndian.PutUint32(header[20:24], uint32(len(compressed)))
	// 20 bytes for the SHA1 hash of the uncompressed data
	copy(header[24:44], []byte(p.SHA1))
	// The remaining 66 bytes are reserved for future use and are already zero

	serialized = append(serialized, header...)

	return append(serialized, compressed...), nil
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
	sha1String := string(header[24:44])

	// Read the compressed frame
	compressed := make([]byte, compressedSize)

	_, err = reader.Read(compressed)

	if err != nil {
		return nil, err
	}

	decompressed, err := decoder.DecodeAll(compressed, nil)

	if err != nil {
		return nil, err
	}

	hash := sha1.New()
	hash.Write(decompressed)
	calculatedSha1 := fmt.Sprintf("%x", hash.Sum(nil))

	if calculatedSha1 != sha1String {
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
