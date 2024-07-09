package storage

import (
	"encoding/gob"
	"fmt"
	"litebase/internal/config"
	"litebase/internal/storage"
	"log"
	"net/http"
)

type Storage struct {
	commandProcessor *CommandProcessor
}

func New() *Storage {
	return &Storage{
		commandProcessor: NewCommandProcessor(),
	}
}

func (s *Storage) CreateConnection(url, id string) {
	CreateConnection(s, url, id)
}

func (s *Storage) Init() {
	config.Init()
	gob.Register(storage.StorageConnection{})
	gob.Register(storage.StorageRequest{})
	gob.Register(storage.StorageResponse{})

	http.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var request storage.StorageRequest

		err := gob.NewDecoder(r.Body).Decode(&request)

		if err != nil {
			log.Println("Error decoding request:", err)
			w.Write([]byte(err.Error()))
			return
		}

		response := s.commandProcessor.Run(request)

		enc := gob.NewEncoder(w)

		err = enc.Encode(response)

		if err != nil {
			log.Println("Error encoding response:", err)
			w.Write([]byte(err.Error()))
			return
		}
	})

	http.HandleFunc("POST /connection", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var connection storage.StorageConnection

		err := gob.NewDecoder(r.Body).Decode(&connection)

		if err != nil {
			log.Println("Error decoding connection:", err)
			w.Write([]byte(err.Error()))
			return
		}

		CreateConnection(s, connection.Url, connection.Id)
	})
}

func (s *Storage) Serve() {
	port := config.Get().StoragePort

	log.Println("Litebase Storage running on port", port)

	http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
}
