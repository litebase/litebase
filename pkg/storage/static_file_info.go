package storage

import (
	"encoding/binary"
	"io/fs"
	"strings"
	"time"

	"github.com/litebase/litebase/internal/utils"
)

/*
StaticFileInfo is a struct that implements the fs.FileInfo interface and can be
endcoded/decoded for distributed file operations.

When encoded the file info will have the following binary format:

| Offset | Length | Description                      |
|--------|--------|----------------------------------|
| 0      | 4      | The length of the name           |
| 4      | 8      | The size of the file              |
| 12     | 8      | The last modified time of the file |
| 20     | n      | The name of the file              |
*/
type StaticFileInfo struct {
	StaticName    string
	StaticSize    int64
	StaticModTime time.Time
}

func NewStaticFileInfo(name string, size int64, modTime time.Time) StaticFileInfo {
	return StaticFileInfo{
		StaticName:    name,
		StaticSize:    size,
		StaticModTime: modTime,
	}
}

func DecodeStaticFileInfo(data []byte) StaticFileInfo {
	info := StaticFileInfo{}

	if len(data) == 0 {
		return info
	}

	nameLength, err := utils.SafeUint32ToInt32(binary.LittleEndian.Uint32(data[0:4]))

	if err != nil {
		return StaticFileInfo{}
	}

	size, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[4:12]))

	if err != nil {
		return StaticFileInfo{}
	}

	modTimeInt64, err := utils.SafeUint64ToInt64(binary.LittleEndian.Uint64(data[12:20]))

	if err != nil {
		return StaticFileInfo{}
	}

	modTime := time.Unix(int64(modTimeInt64), 0).UTC()

	if nameLength == 0 {
		return info
	}

	info.StaticName = string(data[20 : 20+nameLength])
	info.StaticSize = size
	info.StaticModTime = modTime

	return info
}

func (fi StaticFileInfo) Encode() []byte {
	data := make([]byte, 20+len(fi.StaticName))

	uintNameLen, err := utils.SafeIntToUint32(len(fi.StaticName))

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint32(data[0:4], uintNameLen)

	uint64Size, err := utils.SafeInt64ToUint64(fi.StaticSize)

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint64(data[4:12], uint64Size)

	uint64ModTime, err := utils.SafeInt64ToUint64(fi.StaticModTime.UTC().Unix())

	if err != nil {
		return nil
	}

	binary.LittleEndian.PutUint64(data[12:20], uint64ModTime)

	copy(data[20:], []byte(fi.StaticName))

	return data
}

func (fi StaticFileInfo) IsDir() bool {
	// Check if name ends with a slash
	return strings.HasSuffix(fi.StaticName, "/")
}

func (fi StaticFileInfo) IsEmpty() bool {
	return fi.Name() == "" && fi.Size() == 0 && fi.ModTime().IsZero()
}

func (fi StaticFileInfo) Name() string {
	return fi.StaticName
}

func (fi StaticFileInfo) Size() int64 {
	return fi.StaticSize
}

func (fi StaticFileInfo) Mode() fs.FileMode {
	return 0
}

func (fi StaticFileInfo) ModTime() time.Time {
	return fi.StaticModTime
}

func (fi StaticFileInfo) Sys() interface{} {
	return nil
}
