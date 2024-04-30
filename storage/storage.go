package storage

import (
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"litebasedb/internal/config"
	"litebasedb/internal/storage"
	"log"
	"net/http"
	"os"
)

var pageCache = map[string][]byte{}
var pageReadCount = 0

type Storage struct{}

func dataPath(databaseUuid, branchUuid string) string {
	return fmt.Sprintf("%s/.litebasedb/_databases/%s/%s", config.Get().DataPath, databaseUuid, branchUuid)
}

// TODO: Close idle connections
func (s *Storage) Start() {
	config.Init()
	gob.Register(storage.StorageRequest{})
	gob.Register(storage.StorageResponse{})

	http.HandleFunc("/connection", func(w http.ResponseWriter, r *http.Request) {
		close := make(chan struct{})
		defer r.Body.Close()

		enc := gob.NewEncoder(w)

		// Read the messages from the request body
		go func() {
			dec := gob.NewDecoder(r.Body)

			for {
				select {
				case <-close:
					return
				default:
					var request storage.StorageRequest
					var response storage.StorageResponse

					if err := dec.Decode(&request); err != nil {
						close <- struct{}{}
						return
					}

					if request.Command == "READ" {
						data, err := read(request.DatabaseUuid, request.BranchUuid, request.Key, request.Page)
						response.Data = data

						if err != nil {
							response.Error = err.Error()
						}
					}

					if request.Command == "WRITE" {
						err := write(request.DatabaseUuid, request.BranchUuid, request.Key, request.Data)

						if err != nil {
							response.Error = err.Error()
						}
					}

					if err := enc.Encode(response); err != nil {
						log.Println("Error encoding message:", err)
						return
					}

					w.(http.Flusher).Flush()
				}
			}
		}()

		<-close
		log.Println("Closing connection")
	})

	// Read
	http.HandleFunc("GET /databases/{databaseUuid}/{branchUuid}/{file}/pages/{page}", func(w http.ResponseWriter, r *http.Request) {
		databaseUuid := r.PathValue("databaseUuid")
		branchUuid := r.PathValue("branchUuid")
		page := r.PathValue("page")

		hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s", r.PathValue("databaseUuid"), r.PathValue("branchUuid"), r.PathValue("file"), r.PathValue("page"))))

		if data, ok := pageCache[fmt.Sprintf("%x", hash)]; ok {
			w.Write(data)
			return
		}

		data, err := os.ReadFile(fmt.Sprintf("%s/%x", dataPath(databaseUuid, branchUuid), hash))

		if err != nil {
			if os.IsNotExist(err) {
				if page == "1" {
					os.MkdirAll(dataPath(databaseUuid, branchUuid), 0755)
					w.Write([]byte{})
					return
				}

				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		pageReadCount += 1
		fmt.Printf("Read %d pages\n", pageReadCount)

		w.Write(data)
	})

	// Write
	http.HandleFunc("POST /databases/{databaseUuid}/{branchUuid}/{file}/pages/{page}", func(w http.ResponseWriter, r *http.Request) {
		databaseUuid := r.PathValue("databaseUuid")
		branchUuid := r.PathValue("branchUuid")
		hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s", r.PathValue("databaseUuid"), r.PathValue("branchUuid"), r.PathValue("file"), r.PathValue("page"))))
		data, err := io.ReadAll(r.Body)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

	writeFile:
		err = os.WriteFile(fmt.Sprintf("%s/%x", dataPath(databaseUuid, branchUuid), hash), data, 0644)

		if err != nil {
			if os.IsNotExist(err) {
				os.MkdirAll(dataPath(databaseUuid, branchUuid), 0755)
				goto writeFile
			}

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		pageCache[fmt.Sprintf("%x", hash)] = data
	})

	// Size
	http.HandleFunc("GET /databases/{databaseUuid}/{branchUuid}/{file}/size", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("Hello, %s", r.URL.Path)))
	})

	// Delete
	http.HandleFunc("DELETE /databases/{databaseUuid}/{branchUuid}/{file}", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("Hello, %s", r.URL.Path)))
	})

	// Truncate
	http.HandleFunc("POST /databases/{databaseUuid}/{branchUuid}/{file}/truncate", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprintf("Hello, %s", r.URL.Path)))
	})

	http.ListenAndServe(":8082", nil)
}

func read(
	databaseUuid string,
	branchUuid string,
	key string,
	page int64,
) ([]byte, error) {
	data, err := os.ReadFile(fmt.Sprintf("%s/%s", dataPath(databaseUuid, branchUuid), key))

	if err != nil {
		if os.IsNotExist(err) {
			if page == 1 {
				os.MkdirAll(dataPath(databaseUuid, branchUuid), 0755)
				return []byte{}, nil
			}
		}
	}

	return data, nil
}

func write(
	databaseUuid string,
	branchUuid string,
	key string,
	data []byte,
) error {
writeFile:
	err := os.WriteFile(fmt.Sprintf("%s/%s", dataPath(databaseUuid, branchUuid), key), data, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dataPath(databaseUuid, branchUuid), 0755)
			goto writeFile
		}

		return err
	}

	return nil
}
