package storage

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"litebase/internal/storage"
	"log"
	"net/http"
	"time"
)

func CreateConnection(s *Storage, url, connectionId string) {
	reader, writer := io.Pipe()

	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 899*time.Second)

	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/storage/connections/%s", url, connectionId), reader)

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
		timer := time.NewTimer(3000 * time.Millisecond)
		defer timer.Stop()

		for {
			select {
			case <-resetTimeout:
				timer.Reset(3000 * time.Millisecond)
			case <-timer.C:
				cancel()
				close(done)
				return
			}
		}
	}()

	for {
		var request storage.StorageRequest

		if err := dec.Decode(&request); err != nil {
			// Connection closed
			return
		}

		response := s.commandProcessor.Run(request)

		response.Id = request.Id

		if err := enc.Encode(&response); err != nil {
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
