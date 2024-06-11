package storage

import (
	"encoding/gob"
	"fmt"
	"io"
	"litebase/internal/storage"
	"log"
	"net/http"
)

// TODO: Close idle connections
type StorageConnection struct {
	client          *http.Client
	close           chan struct{}
	encoder         *gob.Encoder
	open            bool
	responseChannel chan storage.StorageResponse
	url             string
}

func NewStorageConnection(url string) *StorageConnection {
	gob.Register(storage.StorageRequest{})
	gob.Register(storage.StorageResponse{})

	connection := &StorageConnection{
		close:           make(chan struct{}),
		responseChannel: make(chan storage.StorageResponse),
		url:             url,
	}

	return connection
}

func (c *StorageConnection) Open() error {
	c.client = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	reader, writer := io.Pipe()
	c.encoder = gob.NewEncoder(writer)
	c.close = make(chan struct{})
	c.responseChannel = make(chan storage.StorageResponse)

	var headers map[string]interface{}

	req, err := http.NewRequest("POST", c.url, reader)

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

		c.open = true

		defer response.Body.Close()

		dec := gob.NewDecoder(response.Body)

		for {
			select {
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

	return nil
}

func (c *StorageConnection) Close() {
	c.open = false
	c.client.CloseIdleConnections()
	c.client = nil
	close(c.close)
	close(c.responseChannel)
}

func (c *StorageConnection) Send(message storage.StorageRequest) (storage.StorageResponse, error) {
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

	response := <-c.responseChannel

	return response, nil
}
