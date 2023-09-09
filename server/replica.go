package server

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// Constants
const (
	ConnectionRetryInterval = 1 * time.Second
)

type Replica struct {
	connected       bool
	writes          chan []byte
	reader          *io.PipeReader
	replicationData chan []byte
	writer          *io.PipeWriter
}

func NewReplica() *Replica {
	replica := &Replica{
		replicationData: make(chan []byte),
		writes:          make(chan []byte),
	}

	return replica
}

func (r *Replica) Close() {
	r.connected = false

	if r.reader != nil {
		r.reader.Close()
	}

	if r.writer != nil {
		r.writer.Close()
	}
}

// Create a new connection to the primary
func (r *Replica) Connect() {
	r.reader, r.writer = io.Pipe()
	body := io.NopCloser(r.reader)

	client := &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	request, err := http.NewRequest("POST", fmt.Sprintf("http://%s/%s", os.Getenv("PRIMARY"), "replication"), body)
	request.Header.Set("X-Replica-Id", os.Getenv("LITEBASEDB_PORT"))

	if err != nil {
		log.Println(err)
		return
	}

	r.SubscribeToWrites()
	log.Println("Sending request")

	// go func() {
	// 	time.Sleep(1 * time.Second)
	// 	r.Write([]byte("{}"))
	// 	log.Println("Write")
	// }()

	// TODO: Authenticate the connection
	response, err := client.Do(request)

	if err != nil {
		log.Println(err)
		return
	}

	if response.StatusCode != 200 {
		log.Printf("Connection failed with status code %d\n", response.StatusCode)
		return
	}

	r.connected = true

	defer func() {
		response.Body.Close()
		r.Close()
	}()

	scanner := bufio.NewScanner(response.Body)

	for scanner.Scan() {
		line := scanner.Text()
		log.Println("Received Replication Data: ", line)
		// TODO: Handle the replication data
		// r.replicationData <- []byte(line)
	}
}

func (r *Replica) Read() chan []byte {
	return r.writes
}

// Start the replica server and if not connected to the primary, attempt to connect
func (r *Replica) Run() {
	go func() {
		for {
			if !r.connected {
				log.Println("Attempting to connect to primary")
				r.Connect()
			}

			log.Printf("Connection closed, retrying in %s\n", ConnectionRetryInterval.String())
			// TODO: Add exponential backoff strategy
			time.Sleep(ConnectionRetryInterval)
		}
	}()
}

func (r *Replica) SubscribeToWrites() {
	go func() {
		for data := range r.Read() {
			_, err := r.writer.Write(data)

			if err != nil {
				log.Println(err)
			}
		}
	}()
}

func (r *Replica) Write(data []byte) {
	encoded := []byte(base64.StdEncoding.EncodeToString(data))
	r.writes <- []byte(fmt.Sprintf("%s\n", encoded))
}
