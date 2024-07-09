package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"litebase/internal/storage"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TODO: Close idle connections
type StorageConnection struct {
	activated       chan bool
	client          *http.Client
	close           chan struct{}
	context         context.Context
	encoder         *gob.Encoder
	Id              string
	mutext          *sync.RWMutex
	open            bool
	responseChannel chan storage.StorageResponse
	responseWriter  http.ResponseWriter
	url             string
}

func NewStorageConnection(ctx context.Context, url string) *StorageConnection {
	connection := &StorageConnection{
		Id:              uuid.NewString(),
		activated:       make(chan bool),
		close:           make(chan struct{}),
		context:         ctx,
		mutext:          &sync.RWMutex{},
		responseChannel: make(chan storage.StorageResponse),
		url:             url,
	}

	return connection
}

func (c *StorageConnection) Open() error {
	headers := make(map[string]interface{})

	c.client = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	reader, writer := io.Pipe()
	encoder := gob.NewEncoder(writer)
	c.close = make(chan struct{})

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/connection", c.url), reader)

	if err != nil {
		fmt.Println(err)

		return err
	}

	for key, value := range headers {
		switch v := value.(type) {
		case string:
			req.Header.Add(key, v)
		case float32:
		case float64:
		case int:
		case int8:
		case int16:
		case int32:
		case int64:
			req.Header.Add(key, fmt.Sprintf("%d", int(v)))
		case bool:
			req.Header.Add(key, fmt.Sprintf("%t", v))
		default:
			req.Header.Add(key, fmt.Sprintf("%v", v))
		}
	}

	go func() {
		response, err := c.client.Do(req)

		if err != nil {
			log.Println("Error sending request:", err)
			c.Close()
			return
		}

		defer response.Body.Close()

		dec := gob.NewDecoder(response.Body)

		for {
			select {
			case <-c.context.Done():
				return
			case <-c.close:
				return
			default:
				var response storage.StorageResponse

				if err := dec.Decode(&response); err != nil {
					log.Println("Error decoding message:", err)
					c.Close()

					return
				}

				c.responseChannel <- response
			}
		}
	}()

	// TODO: We need to ensure we are connected first before sending the connection message
	err = encoder.Encode(storage.StorageConnection{
		Id: c.Id,
		// TODO: Need to send a return address
		Url: "http://localhost:8081",
	})

	if err != nil {
		log.Println("Error encoding connection message:", err)
		return err
	}

	select {
	case <-c.activated:
		c.open = true
		return nil
	case <-time.After(3 * time.Second):
		return fmt.Errorf("timeout waiting for connection")
	}
}

func (c *StorageConnection) Close() {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	if !c.open {
		return
	}

	c.open = false
	c.client.CloseIdleConnections()
	c.client = nil
}

func (c *StorageConnection) Send(message storage.StorageRequest) (storage.StorageResponse, error) {
	// Create an Id for the message if one is not provided
	if message.Id == "" {
		message.Id = uuid.NewString()
	}

	if !StorageInit {
		data := bytes.NewBuffer(nil)
		enc := gob.NewEncoder(data)
		enc.Encode(message)
		response, err := http.Post("http://localhost:8085/command", "application/gob", data)

		if err != nil {
			log.Println(err)
			return storage.StorageResponse{}, err
		}

		defer response.Body.Close()

		dec := gob.NewDecoder(response.Body)

		var responseMessage storage.StorageResponse

		if err := dec.Decode(&responseMessage); err != nil {
			log.Println(err)
			c.Close()
			return storage.StorageResponse{}, err
		}

		return responseMessage, nil
	}

	if !c.open {
		err := c.Open()

		if err != nil {
			log.Println("Error opening connection:", err)
			return storage.StorageResponse{}, err
		}
	}

	if err := c.encoder.Encode(message); err != nil {
		log.Println("Error encoding message:", err)
		c.Close()
		return storage.StorageResponse{}, err
	}

	c.responseWriter.(http.Flusher).Flush()

	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			return storage.StorageResponse{}, fmt.Errorf("timeout waiting for response")
		case <-c.close:
			return storage.StorageResponse{}, fmt.Errorf("connection closed")
		case response := <-c.responseChannel:
			return response, nil
		}
	}
}
