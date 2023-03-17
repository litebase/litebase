package server

import (
	"encoding/base64"
	"fmt"
	"log"
)

type Primary struct {
	replicas           map[string]string
	writes             chan []byte
	writesFromReplicas chan []byte
}

func NewPrimary() *Primary {
	primary := &Primary{
		replicas:           make(map[string]string),
		writes:             make(chan []byte),
		writesFromReplicas: make(chan []byte),
	}

	return primary
}

// Add replica to the primary
func (p *Primary) AddReplica(key, value string) {
	p.replicas[key] = value
}

func (p *Primary) Read() chan []byte {
	return p.writes
}

func (p *Primary) ReadFromReplicas() chan []byte {
	log.Println("reading from replica")
	return p.writesFromReplicas
}

// Run replication
func (p *Primary) Run() {
	go func() {
		for data := range p.ReadFromReplicas() {
			decoded, err := base64.StdEncoding.DecodeString(string(data))

			if err != nil {
				log.Println("Error decoding data", err)
			}

			log.Println("Received: ", string(decoded))

			// writeData(fmt.Sprintf("./data/%s/%d", os.Getenv("PORT"), time.Now().UnixNano()), decoded)
			// default:
			// log.Println("No data to replicate")
		}
	}()
}

func (p *Primary) RemoveReplica(key string) {
	delete(p.replicas, key)
}

func (p *Primary) Write(data []byte) {
	if len(p.replicas) > 0 {
		encoded := []byte(base64.StdEncoding.EncodeToString(data))
		p.writes <- []byte(fmt.Sprintf("%s\n", encoded))
	}
}

func (p *Primary) WriteFromReplica(data []byte) {
	p.writesFromReplicas <- data

	// Do some stuff with the data

	// Write to the primary
	p.Write(data)
}
