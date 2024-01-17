package main

import (
	"encoding/json"
	"litebasedb/storage"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joho/godotenv"
)

type Event struct {
	Id               string                    `json:"id"`
	Type             EventType                 `json:"type"`
	DatabaseRequest  storage.DatabaseRequest   `json:"databaseRequest"`
	FilesytemRequest storage.FilesystemRequest `json:"filesystemRequest"`
}

type EventType string

const (
	EventTypeFilesystem EventType = "fs"
	EventTypeDatabase   EventType = "db"
)

type Response struct {
	Id                 string                     `json:"id"`
	DatabaseResponse   storage.DatabaseResponse   `json:"databaseResponse"`
	FilesystemResponse storage.FilesystemResponse `json:"filesystemResponse"`
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load()
}

func Handler(event Event) (Response, error) {
	var response Response

	if event.Type == EventTypeFilesystem {
		filesystemResponse, err := storage.FilesystemRequestHandler(event.Id, event.FilesytemRequest)

		if err != nil {
			log.Println(err)
			return response, err
		}

		response.FilesystemResponse = filesystemResponse
	}

	if event.Type == EventTypeDatabase {
		log.Println("Database request")
		databaseResponse, err := storage.DatabaseRequestHandler(event.Id, event.DatabaseRequest)

		if err != nil {
			log.Println(err)
			return response, err
		}

		response.DatabaseResponse = databaseResponse
	}

	return response, nil
}

func main() {
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.Start(Handler)
	} else {
		http.HandleFunc("/2015-03-31/functions/function/invocations", func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				var event Event
				err := json.NewDecoder(r.Body).Decode(&event)

				if err != nil {
					log.Println(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				// Call the lambda handler with the event
				res, err := Handler(event)

				if err != nil {
					log.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				x, err := json.Marshal(res)

				if err != nil {
					log.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.Write(x)
			}
		})

		log.Println(http.ListenAndServe(":8081", nil))
	}
}
