package storage

import (
	"io"
	internalStorage "litebase/internal/storage"
	"log"
)

// TODO: Create a hash of the file to ensure consistency or ensure permissions are consistent
var dfsFiles map[string]internalStorage.File

/*
Handle a distributed storage request and return the appropriate response.
*/
func HandleDistributedStorageRequest(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	if dfsFiles == nil {
		dfsFiles = make(map[string]internalStorage.File)
	}

	switch dfsRequest.Command {
	case ConnectionStorageCommand:
		dfsResponse = handleDFSConnection(dfsRequest, dfsResponse)
	case CloseStorageCommand:
		dfsResponse = handleDFSClose(dfsRequest, dfsResponse)
	case CreateStorageCommand:
		dfsResponse = handleDFSCreate(dfsRequest, dfsResponse)
	case MkdirStorageCommand:
		dfsResponse = handleDFSMkdir(dfsRequest, dfsResponse)
	case MkdirAllStorageCommand:
		dfsResponse = handleDFSMkdirAll(dfsRequest, dfsResponse)
	case OpenStorageCommand:
		dfsResponse = handleDFSOpen(dfsRequest, dfsResponse)
	case OpenFileStorageCommand:
		dfsResponse = handleDFSOpenFile(dfsRequest, dfsResponse)
	case ReadStorageCommand:
		dfsResponse = handleDFSRead(dfsRequest, dfsResponse)
	case ReadAtStorageCommand:
		dfsResponse = handleDFSReadAt(dfsRequest, dfsResponse)
	case ReadDirStorageCommand:
		dfsResponse = handleDFSReadDir(dfsRequest, dfsResponse)
	case ReadFileStorageCommand:
		dfsResponse = handleDFSReadFile(dfsRequest, dfsResponse)
	case RemoveStorageCommand:
		dfsResponse = handleDFSRemove(dfsRequest, dfsResponse)
	case RemoveAllStorageCommand:
		dfsResponse = handleDFSRemoveAll(dfsRequest, dfsResponse)
	case RenameStorageCommand:
		dfsResponse = handleDFSRename(dfsRequest, dfsResponse)
	case SeekStorageCommand:
		dfsResponse = handleDFSSeek(dfsRequest, dfsResponse)
	case StatStorageCommand:
		dfsResponse = handleDFSStat(dfsRequest, dfsResponse)
	case StatFileStorageCommand:
		dfsResponse = handleDFSStatFile(dfsRequest, dfsResponse)
	case SyncStorageCommand:
		dfsResponse = handleDFSSync(dfsRequest, dfsResponse)
	case TruncateStorageCommand:
		dfsResponse = handleDFSTruncate(dfsRequest, dfsResponse)
	case TruncateFileStorageCommand:
		dfsResponse = handleDFSTruncateFile(dfsRequest, dfsResponse)
	case WriteStorageCommand:
		dfsResponse = handleDFSWrite(dfsRequest, dfsResponse)
	case WriteAtStorageCommand:
		dfsResponse = handleDFSWriteAt(dfsRequest, dfsResponse)
	case WriteFileStorageCommand:
		dfsResponse = handleDFSWriteFile(dfsRequest, dfsResponse)
	case WriteStringStorageCommand:
		dfsResponse = handleDFSWriteString(dfsRequest, dfsResponse)
	default:
		log.Println("Unknown command:", dfsRequest.Command)
	}

	dfsResponse.Command = dfsRequest.Command
	dfsResponse.Path = dfsRequest.Path

	return dfsResponse
}

/*
No action is taken for a connection request, simply reply to the caller to
confirm the connection is successfully established.
*/
func handleDFSConnection(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	return dfsResponse
}

/*
Close a file that is open in the distributed file system. If the file is not
open, no action is taken.
*/
func handleDFSClose(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if ok {
		err := file.Close()

		if err != nil {
			dfsResponse.Error = err.Error()
		}

		delete(dfsFiles, dfsRequest.Path)
	}

	return dfsResponse
}

/*
Create a file in the distributed file system.
*/
func handleDFSCreate(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, err := TieredFS().Create(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()

		return dfsResponse
	}

	dfsFiles[dfsRequest.Path] = file

	return dfsResponse
}

/*
Create a directory in the distributed file system.
*/
func handleDFSMkdir(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().Mkdir(dfsRequest.Path, dfsRequest.Perm)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Create a directory and all parent directories in the distributed file system.
*/
func handleDFSMkdirAll(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().MkdirAll(dfsRequest.Path, dfsRequest.Perm)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Open a file in the distributed file system.
*/
func handleDFSOpen(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, err := TieredFS().Open(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	if file != nil {
		dfsFiles[dfsRequest.Path] = file
	}

	data, err := io.ReadAll(file)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.Data = data

	return dfsResponse
}

/*
Open a file in the distributed file system.
*/
func handleDFSOpenFile(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, err := TieredFS().OpenFile(dfsRequest.Path, dfsRequest.Flag, dfsRequest.Perm)

	if err != nil {
		dfsResponse.Error = err.Error()

		return dfsResponse
	}

	if file != nil {
		dfsFiles[dfsRequest.Path] = file

		data, err := io.ReadAll(file)

		if err != nil {
			dfsResponse.Error = err.Error()
		}

		dfsResponse.Data = data
	}

	return dfsResponse
}

/*
Read from a file in the distributed file system.
*/
func handleDFSRead(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	data := make([]byte, dfsRequest.Length)
	n, err := file.Read(data)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.BytesProcessed = n
	dfsResponse.Data = data

	return dfsResponse
}

/*
Read from a file in the distributed file system at a specific offset.
*/
func handleDFSReadAt(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	buffer := make([]byte, dfsRequest.Length)

	n, err := file.ReadAt(buffer, dfsRequest.Offset)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.BytesProcessed = n
	dfsResponse.Data = buffer

	return dfsResponse
}

/*
Read the contents of a directory in the distributed file system.
*/
func handleDFSReadDir(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	entries, err := TieredFS().ReadDir(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.Entries = entries

	return dfsResponse
}

/*
Read the contents of a file in the distributed file system.
*/
func handleDFSReadFile(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	data, err := TieredFS().ReadFile(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.Data = data

	return dfsResponse
}

/*
Remove a file in the distributed file system.
*/
func handleDFSRemove(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().Remove(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Remove all files in the distributed file system.
*/
func handleDFSRemoveAll(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().RemoveAll(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	// TODO: Inform all storage nodes of the the path that
	// was removed so they can remove related filees
	// from their cache.

	return dfsResponse
}

/*
Rename a file in the distributed file system.
*/
func handleDFSRename(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().Rename(dfsRequest.OldPath, dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Seek to a specific offset in a file in the distributed file system.
*/
func handleDFSSeek(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			log.Println(dfsResponse.Error)
			return dfsResponse
		}
	}

	n, err := file.Seek(dfsRequest.Offset, dfsRequest.Whence)

	if err != nil {
		log.Println(err)
		dfsResponse.Error = err.Error()
	}

	dfsResponse.Offset = n

	return dfsResponse
}

/*
Stat a file in the distributed file system.
*/
func handleDFSStat(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	info, err := TieredFS().Stat(dfsRequest.Path)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	if info != nil {
		dfsResponse.FileInfo = NewStaticFileInfo(
			info.Name(),
			info.Size(),
			info.ModTime(),
		)
	}

	return dfsResponse
}

/*
Stat an open file in the distributed file system.
*/
func handleDFSStatFile(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	info, err := file.Stat()

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.FileInfo = NewStaticFileInfo(
		info.Name(),
		info.Size(),
		info.ModTime(),
	)
	return dfsResponse
}

/*
Sync a file in the distributed file system.
*/
func handleDFSSync(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	err := file.Sync()

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Truncate a file in the distributed file system.
*/
func handleDFSTruncate(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().Truncate(dfsRequest.Path, dfsRequest.Size)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Truncate a file in the distributed file system.
*/
func handleDFSTruncateFile(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	err := file.Truncate(dfsRequest.Size)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Write to a file in the distributed file system.
*/
func handleDFSWrite(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	n, err := file.Write(dfsRequest.Data)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.BytesProcessed = n

	return dfsResponse
}

/*
Write to a file in the distributed file system at a specific offset.
*/
func handleDFSWriteAt(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	n, err := file.WriteAt(dfsRequest.Data, dfsRequest.Offset)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.BytesProcessed = n

	return dfsResponse
}

/*
Write to a file in the distributed file system.
*/
func handleDFSWriteFile(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	err := TieredFS().WriteFile(dfsRequest.Path, dfsRequest.Data, dfsRequest.Perm)

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	return dfsResponse
}

/*
Write a string to a file in the distributed file system.
*/
func handleDFSWriteString(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) DistributedFileSystemResponse {
	file, ok := dfsFiles[dfsRequest.Path]

	if !ok {
		dfsResponse, file = DFSOpenFileForHandler(dfsRequest, dfsResponse)

		if dfsResponse.Error != "" {
			return dfsResponse
		}
	}

	n, err := file.WriteString(string(dfsRequest.Data))

	if err != nil {
		dfsResponse.Error = err.Error()
	}

	dfsResponse.BytesProcessed = n

	return dfsResponse
}

/*
Open a file in the distributed file system and return the file.
*/
func DFSOpenFileForHandler(
	dfsRequest DistributedFileSystemRequest,
	dfsResponse DistributedFileSystemResponse,
) (DistributedFileSystemResponse, internalStorage.File) {
	file, err := TieredFS().OpenFile(dfsRequest.Path, dfsRequest.Flag, dfsRequest.Perm)

	if err != nil {
		dfsResponse.Error = err.Error()

		return dfsResponse, nil
	}

	if file != nil {
		dfsFiles[dfsRequest.Path] = file

		data, err := io.ReadAll(file)

		if err != nil {
			dfsResponse.Error = err.Error()
		}

		dfsResponse.Data = data
	}

	return dfsResponse, file
}
