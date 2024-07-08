package storage

import (
	"errors"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"sync"

	"github.com/google/uuid"
)

type CommandProcessor struct {
	files      map[string]map[string]*os.File
	fileMutext *sync.RWMutex
}

func NewCommandProcessor() *CommandProcessor {
	return &CommandProcessor{
		files:      make(map[string]map[string]*os.File),
		fileMutext: &sync.RWMutex{},
	}
}
func (cp *CommandProcessor) FindFile(path, id string) (*os.File, error) {
	cp.fileMutext.RLock()
	fileMap, ok := cp.files[path]
	cp.fileMutext.RUnlock()

	if !ok {
		return nil, internalStorage.ErrFileIsNotOpened
	}

	file, ok := fileMap[id]

	if !ok {
		return nil, internalStorage.ErrFileIsNotOpened
	}

	return file, nil
}

func (cp *CommandProcessor) OpenFile(path string, flag int, perm fs.FileMode) (string, *os.File, error) {
	file, err := os.OpenFile(path, flag, perm)

	if err != nil {
		return "", nil, err
	}

	id := uuid.New().String()

	cp.fileMutext.Lock()
	defer cp.fileMutext.Unlock()

	if _, ok := cp.files[path]; !ok {
		cp.files[path] = make(map[string]*os.File)
	}

	cp.files[path][id] = file

	return id, file, nil
}

func (cp *CommandProcessor) Run(request internalStorage.StorageRequest) internalStorage.StorageResponse {
	switch request.Command {
	case internalStorage.StorageCommandCreate:
		response := internalStorage.NewStorageResponse()
		_, err := os.Create(request.Path)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandMkdir:
		response := internalStorage.NewStorageResponse()

		err := os.Mkdir(request.Path, 0755)

		if err != nil {
			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandMkdirAll:
		response := internalStorage.NewStorageResponse()

		err := os.MkdirAll(request.Path, 0755)

		if err != nil {
			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandOpen:
		response := internalStorage.NewStorageResponse()

		id, _, err := cp.OpenFile(request.Path, os.O_RDONLY, 0)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		response.FileId = id

		return response

	case internalStorage.StorageCommandOpenFile:
		response := internalStorage.NewStorageResponse()
		id, _, err := cp.OpenFile(request.Path, request.Flag, request.Perm)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		response.FileId = id

		return response

	case internalStorage.StorageCommandReadDir:
		response := internalStorage.NewStorageResponse()

		entries, err := os.ReadDir(request.Path)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		for _, entry := range entries {
			response.DirEntries = append(response.DirEntries, internalStorage.DirEntry{
				Name:  entry.Name(),
				IsDir: entry.IsDir(),
				Type:  entry.Type(),
			})
		}

		return response

	case internalStorage.StorageCommandReadFile:
		response := internalStorage.NewStorageResponse()

		data, err := os.ReadFile(request.Path)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		response.Data = data

		return response

	case internalStorage.StorageCommandRemove:
		response := internalStorage.NewStorageResponse()

		err := os.Remove(request.Path)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandRemoveAll:
		log.Println("Removing all", request.Path)
		response := internalStorage.NewStorageResponse()

		err := os.RemoveAll(request.Path)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandRename:
		response := internalStorage.NewStorageResponse()

		err := os.Rename(request.Path, string(request.Data))

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandStat:
		response := internalStorage.NewStorageResponse()

		info, err := os.Stat(request.Path)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		response.FileInfo = internalStorage.FileInfo{
			Name:    info.Name(),
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
		}

		return response

	case internalStorage.StorageCommandTruncate:
		response := internalStorage.NewStorageResponse()

		err := os.Truncate(request.Path, request.Size)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandWriteFile:
		response := internalStorage.NewStorageResponse()

		err := os.WriteFile(request.Path, request.Data, 0644)

		if err != nil {
			if os.IsNotExist(err) {
				response.Exists = false
			}

			response.Error = err.Error()
		}

		return response

	case internalStorage.StorageCommandFileClose:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		err = file.Close()

		if err != nil {
			response.Error = err.Error()
		}

		cp.fileMutext.Lock()
		delete(cp.files[request.Path], request.FileId)
		cp.fileMutext.Unlock()

		return response

	case internalStorage.StorageCommandFileRead:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		data := make([]byte, request.Size)

		_, err = file.Read(data)

		if err != nil {
			response.Error = err.Error()
			return response
		}

		response.Data = data

		return response
	case internalStorage.StorageCommandFileReadAt:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		data := make([]byte, request.Size)

		_, err = file.ReadAt(data, request.Offset)

		if err != nil {
			response.Error = err.Error()

			return response
		}

		response.Data = data

		return response
	case internalStorage.StorageCommandFileSeek:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		offset, err := file.Seek(request.Offset, request.Whence)

		if err != nil {
			response.Error = err.Error()
		}

		response.Offset = offset

		return response

	case internalStorage.StorageCommandFileWrite:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		n, err := file.Write(request.Data)

		if err != nil {
			response.Error = err.Error()
		}

		response.Length = n

		return response

	case internalStorage.StorageCommandFileWriteAt:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		n, err := file.WriteAt(request.Data, request.Offset)

		if err != nil {
			response.Error = err.Error()
		}

		response.Length = n

		return response

	case internalStorage.StorageCommandFileWriteString:
		response := internalStorage.NewStorageResponse()

		file, err := cp.FindFile(request.Path, request.FileId)

		if err != nil {
			if errors.Is(err, internalStorage.ErrFileIsNotOpened) {
				response.Exists = false
			}

			response.Error = err.Error()

			return response
		}

		n, err := file.WriteString(string(request.Data))

		if err != nil {
			response.Error = err.Error()
		}

		response.Length = n

		return response
	default:
		return internalStorage.StorageResponse{
			Error: internalStorage.ErrInvalidCommand.Error(),
		}
	}
}
