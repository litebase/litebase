package storage

import (
	"fmt"
	"io"
	"os"
)

type FilesystemRequest struct {
	Action  string `json:"action"`
	NewPath string `json:"newPath"`
	Path    string `json:"path"`
	Perm    int    `json:"perm"`
	Flag    int    `json:"flag"`
	Data    []byte `json:"data"`
	Offset  int64  `json:"offset"`
}

type FilesystemResponse struct {
	Path       string                   `json:"path"`
	Bytes      int64                    `json:"bytes"`
	Data       []byte                   `json:"data"`
	DirEntries []map[string]interface{} `json:"dirEntries"`
	Error      string                   `json:"error"`
	Stat       map[string]interface{}   `json:"stat"`
	TotalBytes int64                    `json:"totalBytes"`
}

func FilesystemRequestHandler(id string, request FilesystemRequest) (FilesystemResponse, error) {
	var response FilesystemResponse
	response.Path = request.Path

	if request.Action == "create" {
		file, err := os.Create(request.Path)

		if err != nil {
			response.Error = err.Error()
		} else {
			defer file.Close()

			bytes, err := file.Write(request.Data)

			if err != nil {
				response.Error = err.Error()
			} else {
				response.Bytes = int64(bytes)
			}
		}
	}

	if request.Action == "mkdir" {
		if err := os.Mkdir(request.Path, os.FileMode(request.Perm)); err != nil {
			response.Error = err.Error()
		}
	}

	if request.Action == "mkdirall" {
		if err := os.MkdirAll(request.Path, os.FileMode(request.Perm)); err != nil {
			response.Error = err.Error()
		}
	}

	if request.Action == "open" {
		file, err := os.Open(request.Path)
		if err != nil {
			response.Error = err.Error()
		} else {
			defer file.Close()
			var bytesRead int
			var endOfFile bool
			response.Data = make([]byte, 5*1024*1024)

			for {
				bytes, err := file.Read(response.Data)

				if err != nil {
					if err == io.EOF {
						endOfFile = true
						break
					}

					response.Error = err.Error()
				}

				bytesRead += bytes

				if bytesRead >= len(response.Data) {
					break
				}
			}

			if err == nil {
				response.Data = response.Data[:bytesRead]
				response.Bytes = int64(bytesRead)
				response.TotalBytes = int64(bytesRead)

				if endOfFile {
					fileInfo, err := file.Stat()

					if err != nil {
						response.Error = err.Error()
					} else {
						response.TotalBytes = fileInfo.Size()
					}
				}
			}
		}
	}

	if request.Action == "openfile" {
		file, err := os.OpenFile(request.Path, request.Flag, os.FileMode(request.Perm))

		if err != nil {
			response.Error = err.Error()
		} else {
			defer file.Close()

			response.Data = make([]byte, 1024)

			bytes, err := file.Read(response.Data)

			if err != nil {
				if err != io.EOF {
					response.Error = err.Error()
				}
			} else {
				response.Bytes = int64(bytes)
			}
		}
	}

	if request.Action == "readat" {
		file, err := os.Open(request.Path)

		if err != nil {
			response.Error = err.Error()
		} else {
			defer file.Close()

			_, err := file.ReadAt(request.Data, request.Offset)

			if err != nil {
				if err != io.EOF {
					response.Error = err.Error()
				}
			} else {
				response.Data = request.Data
				response.Bytes = int64(len(request.Data))
			}
		}
	}

	if request.Action == "readdir" {
		dirEntries, err := os.ReadDir(request.Path)

		if err != nil {
			response.Error = err.Error()
		} else {
			data := []map[string]interface{}{}
			for _, entry := range dirEntries {
				fileInfo, err := os.Stat(fmt.Sprintf("%s/%s", request.Path, entry.Name()))

				if err != nil {
					response.Error = err.Error()
					break
				}

				data = append(data, map[string]interface{}{
					"name":  entry.Name(),
					"isDir": entry.IsDir(),
					"type":  int32(entry.Type()),
					"info": map[string]interface{}{
						"mode":    int32(fileInfo.Mode()),
						"size":    fileInfo.Size(),
						"isDir":   entry.IsDir(),
						"modTime": fileInfo.ModTime(),
					},
				})
			}

			response.DirEntries = data
		}
	}

	if request.Action == "readfile" {
		data, err := os.ReadFile(request.Path)

		if err != nil {
			response.Error = err.Error()
		} else {
			response.Data = data
		}
	}

	if request.Action == "remove" {
		if err := os.Remove(request.Path); err != nil {
			response.Error = err.Error()
		}
	}

	if request.Action == "removeall" {
		if err := os.RemoveAll(request.Path); err != nil {
			response.Error = err.Error()
		}
	}

	if request.Action == "rename" {
		if err := os.Rename(request.Path, request.NewPath); err != nil {
			response.Error = err.Error()
		}
	}

	if request.Action == "stat" {
		fileInfo, err := os.Stat(request.Path)

		if err != nil {
			response.Error = err.Error()
		} else {
			response.Stat = map[string]interface{}{
				"mode":    int32(fileInfo.Mode()),
				"size":    fileInfo.Size(),
				"isDir":   fileInfo.IsDir(),
				"modTime": fileInfo.ModTime(),
			}
		}
	}

	if request.Action == "write" {
		file, err := os.OpenFile(request.Path, os.O_APPEND|os.O_WRONLY, os.FileMode(request.Perm))

		if err != nil {
			response.Error = err.Error()
		} else {
			defer file.Close()

			bytes, err := file.Write(request.Data)

			if err != nil {
				response.Error = err.Error()
			} else {
				response.Bytes = int64(bytes)
			}
		}
	}

	if request.Action == "writeat" {
		file, err := os.OpenFile(request.Path, os.O_CREATE|os.O_RDWR, 0666)

		if err != nil {
			response.Error = err.Error()
		} else {
			defer file.Close()

			bytes, err := file.WriteAt(request.Data, request.Offset)

			if err != nil {
				response.Error = err.Error()
			} else {
				response.Bytes = int64(bytes)
			}
		}
	}

	if request.Action == "writefile" {
		err := os.WriteFile(request.Path, request.Data, os.FileMode(request.Perm))

		if err != nil {
			response.Error = err.Error()
		} else {
			response.Bytes = int64(len(request.Data))
		}
	}

	return response, nil
}
