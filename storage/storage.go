package storage

import (
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
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
func (s *Storage) Init() {
	config.Init()
	gob.Register(storage.StorageRequest{})
	gob.Register(storage.StorageResponse{})

	http.HandleFunc("POST /connection", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data := map[string]interface{}{}

		err := json.NewDecoder(r.Body).Decode(&data)

		if err != nil {
			log.Println("Error decoding message:", err)
			return
		}

		connectionId := data["connection_id"].(string)
		connectionUrl := data["connection_url"].(string)
		log.Println("Opening connection")

		CreateConnection(connectionUrl, connectionId)

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
}

func (s *Storage) Serve() {
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
	// TODO: Write the data to a versioned page format
	pageData := data

writeFile:
	err := os.WriteFile(fmt.Sprintf("%s/%s", dataPath(databaseUuid, branchUuid), key), pageData, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dataPath(databaseUuid, branchUuid), 0755)
			goto writeFile
		}

		return err
	}

	return nil
}
