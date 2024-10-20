package storage

import (
	"encoding/binary"
	internalStorage "litebase/internal/storage"
	"log"
)

/*
This response represents the response from a distributed file system operation.
When encoded the response will have the following binary format:

| Offset | Length | Description                            |
|--------|--------|----------------------------------------|
| 0      | 4      | The command that was called            |
| 4      | 4      | The length of the data being returned  |
| 8      | 4      | The length of the bytes processed      |
| 12     | 4      | The length of the entries bytes        |
| 16     | 8      | The return offset				       |
| 24     | 4      | The length of error message            |
| 28     | 4      | The length of the path                 |
| 32     | 4      | The length of the fileinfo             |
| 36     | n      | The data                               |
| 36+n   | m      | The entries                            |
| 36+n+m | o      | The error message                      |
| 36+n+m+o | p    | The path                               |
| 36+n+m+o+p | q  | The fileinfo                           |
*/
type DistributedFileSystemResponse struct {
	BytesProcessed int
	Command        DistributedStorageCommand
	Data           []byte
	Entries        []internalStorage.DirEntry
	Error          string
	FileInfo       StaticFileInfo
	Offset         int64
	Path           string
}

func DecodeDistributedFileSystemResponse(response DistributedFileSystemResponse, data []byte) DistributedFileSystemResponse {
	response = response.Reset()

	response.Command = DistributedStorageCommand(int(binary.LittleEndian.Uint32(data[0:4])))

	if len(data) <= 4 {
		return response
	}

	dataLength := int(binary.LittleEndian.Uint32(data[4:8]))
	response.BytesProcessed = int(binary.LittleEndian.Uint32(data[8:12]))
	entryLength := int(binary.LittleEndian.Uint32(data[12:16]))
	response.Offset = int64(binary.LittleEndian.Uint64(data[16:24]))
	errorLength := int(binary.LittleEndian.Uint32(data[24:28]))
	pathLength := int(binary.LittleEndian.Uint32(data[28:32]))
	fileInfoLength := int(binary.LittleEndian.Uint32(data[32:36]))

	var offset = 36

	if dataLength > 0 {
		response.Data = data[offset : offset+dataLength]
		offset += dataLength
	}

	if entryLength > 0 {
		response.Entries = DecodeDirEntries(data[offset : offset+entryLength])
		offset += entryLength
	}

	if errorLength > 0 {
		response.Error = string(data[offset : offset+errorLength])
		offset += errorLength
	}

	if pathLength > 0 {
		response.Path = string(data[offset : offset+pathLength])
		offset += pathLength
	}

	if fileInfoLength > 0 {
		response.FileInfo = DecodeStaticFileInfo(data[offset : offset+fileInfoLength])
	}

	return response
}

func DecodeDirEntries(data []byte) []internalStorage.DirEntry {
	entries := make([]internalStorage.DirEntry, 0)

	// Read the number of entries
	numEntries := binary.LittleEndian.Uint32(data[0:4])

	if numEntries == 0 {
		return entries
	}

	data = data[4:]

	for i := uint32(0); i < numEntries; i++ {
		entryLength := binary.LittleEndian.Uint32(data[0:4])
		entryData := data[4 : 4+entryLength]
		data = data[4+entryLength:]

		entries = append(entries, internalStorage.DecodeDirEntry(entryData))
	}

	return entries
}

func EncodeDirEntries(entries []internalStorage.DirEntry) []byte {
	data := make([]byte, 4) // Initialize with capacity for the number of entries

	binary.LittleEndian.PutUint32(data[0:4], uint32(len(entries)))

	for _, entry := range entries {
		entryData := entry.Encode()
		entryLength := make([]byte, 4)
		binary.LittleEndian.PutUint32(entryLength, uint32(len(entryData)))
		data = append(data, entryLength...)
		data = append(data, entryData...)
	}

	log.Println("Encoded dir entries:", data)

	return data
}

func (dfr DistributedFileSystemResponse) Encode() []byte {
	var entryData []byte
	var fileInfoData []byte
	offset := 36

	if len(dfr.Entries) > 0 {
		entryData = EncodeDirEntries(dfr.Entries)
	}

	if !dfr.FileInfo.IsEmpty() {
		fileInfoData = dfr.FileInfo.Encode()
	}

	data := make([]byte, offset+len(dfr.Data)+len(entryData)+len(dfr.Error)+len(dfr.Path)+len(fileInfoData))

	binary.LittleEndian.PutUint32(data[0:4], uint32(dfr.Command))
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(dfr.Data)))
	binary.LittleEndian.PutUint32(data[8:12], uint32(dfr.BytesProcessed))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(entryData)))
	binary.LittleEndian.PutUint64(data[16:24], uint64(dfr.Offset))
	binary.LittleEndian.PutUint32(data[24:28], uint32(len(dfr.Error)))
	binary.LittleEndian.PutUint32(data[28:32], uint32(len(dfr.Path)))
	binary.LittleEndian.PutUint32(data[32:36], uint32(len(fileInfoData)))

	if len(dfr.Data) > 0 {
		copy(data[offset:offset+len(dfr.Data)], dfr.Data)
		offset += len(dfr.Data)
	}

	if len(entryData) > 0 {
		copy(data[offset:offset+len(entryData)], entryData)
		offset += len(entryData)
	}

	if len(dfr.Error) > 0 {
		copy(data[offset:offset+len(dfr.Error)], dfr.Error)
		offset += len(dfr.Error)
	}

	if len(dfr.Path) > 0 {
		copy(data[offset:offset+len(dfr.Path)], dfr.Path)
		offset += len(dfr.Path)
	}

	if len(fileInfoData) > 0 {
		copy(data[offset:offset+len(fileInfoData)], fileInfoData)
	}

	return data
}

// Check if the response is empty.
func (dffr DistributedFileSystemResponse) IsEmpty() bool {
	return len(dffr.Data) == 0 && dffr.Error == "" && dffr.FileInfo.IsEmpty() && dffr.Path == "" && dffr.BytesProcessed == 0
}

// Reset the response.
func (dffr DistributedFileSystemResponse) Reset() DistributedFileSystemResponse {
	dffr.BytesProcessed = 0
	dffr.Command = 0
	dffr.Data = dffr.Data[:0]       // Reuse the underlying array
	dffr.Entries = dffr.Entries[:0] // Reuse the underlying array
	dffr.Error = ""
	dffr.FileInfo = StaticFileInfo{}
	dffr.Offset = 0
	dffr.Path = ""

	return dffr
}
