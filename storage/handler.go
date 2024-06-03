package storage

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"litebasedb/internal/storage"
	"log"
	"net/http"
	"time"
)

type Event struct {
	Action        string `json:"action"`
	ConnectionId  string `json:"connection_id"`
	ConnectionUrl string `json:"connection_url"`
	Data          string `json:"data"`
}

var commandProcessor = NewCommandProcessor()

func CreateConnection(url, connectionId string) {
	reader, writer := io.Pipe()

	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 899*time.Second)

	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/%s", url, connectionId), reader)

	if err != nil {
		log.Println(err)
		return
	}

	res, err := client.Do(req)

	if err != nil {
		log.Println(err)
		return
	}

	defer res.Body.Close()

	resetTimeout := make(chan bool)
	done := make(chan bool)

	dec := gob.NewDecoder(res.Body)
	enc := gob.NewEncoder(writer)

	// Run a timer to check for timeouts
	go func() {
		timer := time.NewTimer(1000 * time.Millisecond)
		defer timer.Stop()

		for {
			select {
			case <-resetTimeout:
				timer.Reset(1000 * time.Millisecond)
			case <-timer.C:
				cancel()
				close(done)
				return
			}
		}
	}()

	for {

		var request storage.StorageRequest
		var response storage.StorageResponse

		if err := dec.Decode(&request); err != nil {
			// Cannot decode the message when the connection is closed
			return
		}

		data, err := commandProcessor.Run(request)

		if err != nil {
			log.Println("Error processing command:", err)
		}

		response.Id = request.Id
		response.Data = data

		if err != nil {
			response.Error = err.Error()
		}

		if err := enc.Encode(response); err != nil {
			log.Println("Error encoding message:", err)
			return
		}

		// Check if the context has expired
		if ctx.Err() != nil {
			log.Println("Context expired, closing connection")
			break
		}

		resetTimeout <- true
	}
}
