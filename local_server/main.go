package main

import (
	"encoding/json"
	"litebasedb/runtime"
	"litebasedb/runtime/event"
	"log"
	"net/http"
	"os"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func init() {
	os.Setenv("LITEBASEDB_RUNTIME_ID", uuid.NewString())
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	err := godotenv.Load(".env.runtime")

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	server := &http.Server{
		Addr: ":8001",
	}

	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			request := &event.Event{}
			decoder := json.NewDecoder(r.Body)
			decoder.Decode(&request)

			var response interface{}

			if r.Header.Get("X-Amz-Invocation-Type") == "RequestResponse" {
				response = runtime.Handler(request)
			} else {
				go runtime.Handler(request)
			}

			json, err := json.Marshal(response)

			if err != nil {
				panic(err)
			}

			w.Write(json)

			return
		}

		w.Write([]byte(nil))
	})

	log.Fatal(server.ListenAndServe())
}
