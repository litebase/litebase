package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"
)

type LambdaConnectionPool struct {
	availableIds   []string
	connectionHash string
	connectionUrl  string
	connection     *LambdaConnection
	capacity       int
	connections    map[string]*LambdaConnection
	mutex          *sync.Mutex
}

// TODO: There is a bug in this code. There is an infinite wait if a connection cannot be returned.

func NewLambdaConnectionPool(capacity int, connectionHash, connectionUrl string) *LambdaConnectionPool {
	pool := &LambdaConnectionPool{
		availableIds:   []string{},
		capacity:       capacity,
		connectionHash: connectionHash,
		connectionUrl:  connectionUrl,
		connections:    make(map[string]*LambdaConnection),
		mutex:          &sync.Mutex{},
	}

	return pool
}

func (pool *LambdaConnectionPool) Activate(connection *LambdaConnection, w http.ResponseWriter, r *http.Request) error {
	return connection.Open(w, r, func() {
		connection.activated <- true
	})
}

func (pool *LambdaConnectionPool) Create() (*LambdaConnection, error) {
	connection := NewLambdaConnection(pool.connectionHash)

	// pool.mutex.Lock()
	pool.connections[connection.Id] = connection
	// pool.mutex.Unlock()

	// cfg, err := config.LoadDefaultConfig(
	// 	context.TODO(),
	// 	config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
	// 		os.Getenv("FUNC_AK"),
	// 		os.Getenv("FUNC_SK"),
	// 		"",
	// 	)),
	// 	config.WithRegion("us-east-1"),
	// 	config.WithRetryMaxAttempts(3),
	// )

	// if err != nil {
	// 	return err
	// }

	// if os.Getenv("FUNC_ENDPOINT") != "" {
	// 	cfg.BaseEndpoint = aws.String(os.Getenv("FUNC_ENDPOINT"))
	// }

	// var lambdaClient = lambda.NewFromConfig(cfg)

	// jsonData, err := json.Marshal(map[string]string{
	// 	"action":         "create_connection",
	// 	"connection_url": pool.connectionUrl,
	// 	"connection_id":  connection.Id,
	// 	"database_uuid":  os.Getenv("DATABASE_UUID"),
	// })

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// functionName := os.Getenv("FUNC_NAME")

	// // Invoke Lambda function to create a connection
	// _, err = lambdaClient.Invoke(context.TODO(), &lambda.InvokeInput{
	// 	FunctionName:   aws.String(functionName),
	// 	Payload:        jsonData,
	// 	InvocationType: "RequestResponse",
	// })

	data := map[string]string{
		"action":         "create_connection",
		"connection_url": pool.connectionUrl,
		"connection_id":  connection.Id,
		"database_uuid":  "test",
	}

	jsonData, err := json.Marshal(data)

	// Create buffer
	go func() {
		request, err := http.NewRequestWithContext(connection.context, "POST", "http://localhost:8082/connection", bytes.NewBuffer(jsonData))

		if err != nil {
			log.Println(err)

			LambdaConnectionManager().Remove(pool.connectionHash, connection)

			return
		}

		client := &http.Client{}

		_, err = client.Do(request)

		if err != nil {
			// context canceled
			if !errors.Is(err, context.Canceled) {
				log.Println(err)
			}

			LambdaConnectionManager().Remove(pool.connectionHash, connection)
		}
	}()

	return connection, err
}

func (pool *LambdaConnectionPool) Get() (*LambdaConnection, error) {
	pool.mutex.Lock()
	var err error

	// log.Println("No connection...")
	if pool.connection == nil {
		pool.connection, err = pool.Create()

		if err != nil {
			log.Println("Error creating connection", err)
			return nil, err
		}
	}
	pool.mutex.Unlock()

	if pool.connection.opened {
		return pool.connection, nil
	}

	timer := time.NewTimer(5 * time.Second)

	// pool.mutex.Lock()
	// defer pool.mutex.Unlock()

	for {
		select {
		case <-timer.C:
			return nil, errors.New("timeout waiting for available connection")
		case <-pool.connection.activated:
			return pool.connection, nil
		}
	}
}

// func (pool *LambdaConnectionPool) Get() (*LambdaConnection, error) {
// 	pool.mutex.Lock()
// 	var connection *LambdaConnection = nil
// 	// var ok bool
// 	// if len(pool.availableIds) > 0 {
// 	// 	for _, id := range pool.availableIds {
// 	// 		connection, ok = pool.connections[id]

// 	// 		if !ok {
// 	// 			continue
// 	// 		}

// 	// 		if connection.opened {
// 	// 			pool.availableIds = append(pool.availableIds[:i], pool.availableIds[i+1:]...)
// 	// 			pool.mutex.Unlock()

// 	// 			return connection, nil
// 	// 		}
// 	// 	}
// 	// }
// 	if len(pool.connections) > 0 {
// 		for _, connection = range pool.connections {
// 			if connection != nil {
// 				break
// 			}
// 		}
// 	}
// 	pool.mutex.Unlock()

// 	var err error

// 	// If no available connections, create one
// 	// pool.mutex.Lock()
// 	// if len(pool.availableIds) == 0 && len(pool.connections) < pool.capacity {
// 	if len(pool.connections) < pool.capacity {
// 		// pool.mutex.Unlock()
// 		connection, err = pool.Create()

// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	timer := time.NewTimer(5 * time.Second)

// 	for {
// 		select {
// 		case <-timer.C:
// 			return nil, errors.New("timeout waiting for available connection")
// 		case <-connection.activated:
// 			return connection, nil
// 		}
// 	}
// }

func (pool *LambdaConnectionPool) Find(id string) (*LambdaConnection, error) {
	pool.mutex.Lock()
	connection, ok := pool.connections[id]
	pool.mutex.Unlock()

	if !ok {
		return nil, errors.New("connection not found")
	}

	return connection, nil
}

func (pool *LambdaConnectionPool) Release(connection *LambdaConnection) {
	pool.mutex.Lock()
	// Add the connection ID back to the available IDs
	// pool.availableIds = append(pool.availableIds, connection.Id)
	pool.mutex.Unlock()
}

func (pool *LambdaConnectionPool) Remove(connection *LambdaConnection) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	pool.connection = nil

	// Remove the connection from the map
	delete(pool.connections, connection.Id)

	for i, id := range pool.availableIds {
		if id == connection.Id {
			pool.availableIds = append(pool.availableIds[:i], pool.availableIds[i+1:]...)
			break
		}
	}
}
