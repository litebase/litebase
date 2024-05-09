package storage

import (
	"context"
	"encoding/gob"
	"errors"
	"litebasedb/internal/storage"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

type LambdaConnection struct {
	activated          chan bool
	cancel             context.CancelFunc
	close              chan struct{}
	context            context.Context
	encoder            *gob.Encoder
	httpRequest        *http.Request
	httpResponseWriter http.ResponseWriter
	Hash               string
	Id                 string
	mutext             *sync.Mutex
	opened             bool
	reader             chan []byte
	responses          map[string]chan storage.StorageResponse
}

func NewLambdaConnection(hash string) *LambdaConnection {
	gob.Register(storage.StorageRequest{})
	gob.Register(storage.StorageResponse{})
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	return &LambdaConnection{
		activated: make(chan bool),
		cancel:    cancel,
		close:     make(chan struct{}),
		context:   ctx,
		Hash:      hash,
		Id:        uuid.New().String(),
		mutext:    &sync.Mutex{},
		reader:    make(chan []byte),
		responses: make(map[string]chan storage.StorageResponse),
	}
}

func (c *LambdaConnection) listen() error {
	defer c.httpRequest.Body.Close()

	dec := gob.NewDecoder(c.httpRequest.Body)

	for {
		select {
		case <-c.close:
			log.Println("Connection closed")
			return nil
		default:
			var response storage.StorageResponse

			if err := dec.Decode(&response); err != nil {
				// Cannot decode the message when the connection is closed
				c.Close()

				return nil
			}

			// Send the response to the appropriate channel
			c.responses[response.Id] <- response
		}
	}
}

// TODO: Implement the Close method
func (c *LambdaConnection) Close() error {
	c.opened = false

	c.cancel()

	for _, responseChannel := range c.responses {
		close(responseChannel)
	}

	LambdaConnectionManager().Remove(c.Hash, c)

	return nil
}

func (c *LambdaConnection) Open(response http.ResponseWriter, r *http.Request, openCallback func()) error {
	// c.mutext.Lock()
	c.httpRequest = r
	c.httpResponseWriter = response
	c.httpResponseWriter.Header().Set("Transfer-Encoding", "chunked")
	c.httpResponseWriter.Header().Set("Connection", "close")
	c.httpResponseWriter.(http.Flusher).Flush()
	c.encoder = gob.NewEncoder(c.httpResponseWriter)

	if openCallback != nil {
		openCallback()
	}

	c.opened = true

	// c.mutext.Unlock()

	return c.listen()
}

func (c *LambdaConnection) Send(message storage.StorageRequest) (storage.StorageResponse, error) {
	if !c.opened {
		log.Println("Connection is not open")
		return storage.StorageResponse{}, errors.New("connection is not open")
	}

	message.Id = uuid.NewString()

	if err := c.encoder.Encode(message); err != nil {
		log.Println("Error encoding message:", err)
		c.Close()
		return storage.StorageResponse{}, err
	}

	// c.mutext.Lock()
	// Create a new channel to receive the response
	c.responses[message.Id] = make(chan storage.StorageResponse)

	c.httpResponseWriter.(http.Flusher).Flush()

	// Wait for the response on the channel
	response := <-c.responses[message.Id]

	// Remove the channel from the map
	delete(c.responses, message.Id)
	// c.mutext.Unlock()

	return response, nil
}
