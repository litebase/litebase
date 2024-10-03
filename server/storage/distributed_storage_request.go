package storage

import (
	"encoding/binary"
	"io/fs"
)

/*
This request represents a request to the distributed file system. When encoded
the request will have the fillowing binary format:

| Offset | Length | Description                              |
|--------|--------|----------------------------------------- |
| 0      | 4      | The command that is being called         |
| 4      | 4      | The length of the data being transmitted |
| 8      | 4      | The length of the data being requested   |
| 12     | 4      | The flag                                 |
| 16     | 8      | The offset                               |
| 24     | 4      | The length of the old path               |
| 28     | 4      | The length of the path                   |
| 32     | 4      | The permissions                          |
| 36     | 8      | The size                                 |
| 44     | 4      | The whence                               |
| 48     | n      | The bytes of the old path                |
| 48+n   | m      | The  bytes of the path                   |
| 48+n+m | o      | The bytes of the data                    |
*/

type DistributedFileSystemRequest struct {
	Command DistributedStorageCommand
	Data    []byte
	Flag    int
	Length  int
	Offset  int64
	OldPath string
	Path    string
	Perm    fs.FileMode
	Size    int64
	Whence  int
}

func DecodeDistributedFileSystemRequest(request DistributedFileSystemRequest, data []byte) (DistributedFileSystemRequest, error) {
	request = request.Reset()

	request.Command = DistributedStorageCommand(int(binary.LittleEndian.Uint32(data[0:4])))
	dataTransferLength := binary.LittleEndian.Uint32(data[4:8])
	dataRequestLength := binary.LittleEndian.Uint32(data[8:12])

	request.Length = int(dataRequestLength)
	request.Flag = int(binary.LittleEndian.Uint32(data[12:16]))
	request.Offset = int64(binary.LittleEndian.Uint64(data[16:24]))
	request.Perm = fs.FileMode(binary.LittleEndian.Uint32(data[32:36]))
	request.Size = int64(binary.LittleEndian.Uint64(data[36:44]))
	request.Whence = int(binary.LittleEndian.Uint32(data[44:48]))

	oldPathLength := int(binary.LittleEndian.Uint32(data[24:28]))
	pathLength := int(binary.LittleEndian.Uint32(data[28:32]))

	offset := 48

	if oldPathLength > 0 {
		request.OldPath = string(data[offset : offset+oldPathLength])
		offset += oldPathLength
	}

	if pathLength > 0 {
		request.Path = string(data[offset : offset+pathLength])
		offset += pathLength
	}

	if dataTransferLength > 0 {
		request.Data = data[offset : offset+int(dataTransferLength)]
	}

	return request, nil
}

func (dfr DistributedFileSystemRequest) Encode() []byte {
	offset := 48
	dataTransferLength := len(dfr.Data)
	oldPathLength := len(dfr.OldPath)
	pathLength := len(dfr.Path)

	data := make([]byte, offset+dataTransferLength+oldPathLength+pathLength)

	binary.LittleEndian.PutUint32(data[0:4], uint32(dfr.Command))
	binary.LittleEndian.PutUint32(data[4:8], uint32(dataTransferLength))
	binary.LittleEndian.PutUint32(data[8:12], uint32(dfr.Length))
	binary.LittleEndian.PutUint32(data[12:16], uint32(dfr.Flag))
	binary.LittleEndian.PutUint64(data[16:24], uint64(dfr.Offset))
	binary.LittleEndian.PutUint32(data[24:28], uint32(oldPathLength))
	binary.LittleEndian.PutUint32(data[28:32], uint32(pathLength))
	binary.LittleEndian.PutUint32(data[32:36], uint32(dfr.Perm))
	binary.LittleEndian.PutUint64(data[36:44], uint64(dfr.Size))
	binary.LittleEndian.PutUint32(data[44:48], uint32(dfr.Whence))

	if oldPathLength > 0 {
		copy(data[offset:offset+oldPathLength], dfr.OldPath)
		offset += oldPathLength
	}

	if pathLength > 0 {
		copy(data[offset:offset+pathLength], dfr.Path)
		offset += pathLength
	}

	if dataTransferLength > 0 {
		copy(data[offset:offset+dataTransferLength], dfr.Data)
	}

	return data
}

func (dfr DistributedFileSystemRequest) IsEmpty() bool {
	return len(dfr.Data) == 0 && dfr.Flag == 0 && dfr.Length == 0 && dfr.Offset == 0 && dfr.OldPath == "" && dfr.Path == "" && dfr.Perm == 0 && dfr.Size == 0 && dfr.Whence == 0
}

func (dfr DistributedFileSystemRequest) Reset() DistributedFileSystemRequest {
	dfr.Command = 0
	dfr.Data = nil
	dfr.Flag = 0
	dfr.Length = 0
	dfr.Offset = 0
	dfr.OldPath = ""
	dfr.Path = ""
	dfr.Perm = 0
	dfr.Size = 0
	dfr.Whence = 0

	return dfr
}
