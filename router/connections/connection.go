package connections

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"litebasedb/router/auth"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Connection struct {
	branchUuid  string
	close       chan bool
	closedAt    time.Time
	connected   bool
	ConnectedAt time.Time
	// connectionChannel chan struct{}
	connectionHash    string
	connectionId      string
	databaseUuid      string
	handlers          map[string][]func(map[string]interface{})
	messageHash       []byte
	open              bool
	reader            chan string
	Request           *http.Request
	RequestCount      int
	Response          http.ResponseWriter
	ResponseCallbacks map[string]func([]byte)
	writer            chan []byte
}

func NewConnection(connectionId string, connectionHash string, request *http.Request, w http.ResponseWriter) *Connection {
	return &Connection{
		close:             make(chan bool),
		connected:         false,
		connectionHash:    connectionHash,
		connectionId:      connectionId,
		handlers:          make(map[string][]func(map[string]interface{})),
		messageHash:       nil,
		open:              false,
		reader:            make(chan string),
		Request:           request,
		RequestCount:      0,
		Response:          w,
		ResponseCallbacks: make(map[string]func([]byte)),
		writer:            make(chan []byte),
	}
}

func CreateConnection(databaseUuid, branchUuid, connectionKey string, request *http.Request, w http.ResponseWriter) *Connection {
	connectionId := sha1.New().Sum([]byte(uuid.New().String()))
	connectionHash := hmac.New(sha256.New, []byte(connectionKey)).Sum([]byte(connectionId))
	connection := NewConnection(string(connectionId), string(connectionHash), request, w)
	PutConnection("unassigned", "", connection)
	go connection.Run()
	// connection.onCreated()

	return connection
}

func (c *Connection) Authenticate(databaseuuid, branchUuid string) {
	c.messageHash = sha256.New().Sum([]byte(fmt.Sprintf("%s%s", uuid.New().String(), c.connectionId)))
	c.databaseUuid = databaseuuid
	c.branchUuid = branchUuid

	DeleteConnection("unassigned", "", c.connectionId)
	PutConnection(databaseuuid, branchUuid, c)

	c.Send("CONNECTION_READY", nil)
}

/*
Close the connection.
*/
func (c *Connection) Close() bool {
	c.open = false
	c.closedAt = time.Now()

	if c.databaseUuid == "unassigned" {
		DeleteConnection("unassigned", "", c.connectionId)
	} else {
		DeleteConnection(c.databaseUuid, c.branchUuid, c.connectionId)
	}

	// if c.connectionChannel != nil {
	// 	close(c.connectionChannel)
	// }

	c.updateRequestBalance()

	return true
}

func (c *Connection) IsOpen() bool {
	// TODO: check if the websocket connection is open
	return c.open
}

/*
Listen for events on the connection.
*/
func (c *Connection) Listen() {
	close := make(chan bool)

	c.On("message", func(data map[string]interface{}) {
		switch data["event"].(string) {
		case "CONNECTION_AUTH":
			go func(data map[string]interface{}) {
				c.Authenticate(data["databaseUuid"].(string), data["branchUuid"].(string))
			}(data)
		case "CONNECTION_VERIFY":
			go func(data map[string]interface{}) {
				connection, ok := Connections[Key(data["databaseUuid"].(string), data["branchUuid"].(string))][data["connectionId"].(string)]

				if !ok {
					return
				}

				connection.Verify(data["verificationNonce"].(string))
			}(data)
		case "QUERY_RESPONSE":
			go func(data map[string]interface{}) {
				callback, ok := c.ResponseCallbacks[data["responseHash"].(string)]
				jsonString, err := json.Marshal(data["response"])

				if err != nil {
					log.Println(err)
					return
				}

				if ok {
					callback(jsonString)
					// delete(c.ResponseCallbacks, data["responseHash"].(string))
				}
			}(data)
		}
	})

	c.On("close", func(data map[string]interface{}) {
		close <- true
	})

	<-close
}

func (c *Connection) On(event string, handler func(map[string]interface{})) {
	c.handlers[event] = append(c.handlers[event], handler)
}

/*
The on connection created handler.
*/
func (c *Connection) onCreated() {
	c.connected = true
	c.ConnectedAt = time.Now()
	c.Send("CONNECTION_OPEN", nil)
}

func (c *Connection) Run() {
	c.Response.Header().Set("Content-Type", "text/plain")
	c.Response.Header().Set("Transfer-Encoding", "chunked")

	flusher := c.Response.(http.Flusher)

	go c.onCreated()

	go func() {
		buf := make([]byte, 128)
		jsonBlock := ""

		for {
			// if the http request is closed, close the connection
			if c.Request.Body == nil {
				c.close <- true
				break
			}

			n, err := c.Request.Body.Read(buf)

			if err != nil {
				c.close <- true
				break
			}

			jsonBlock += string(buf[:n])

			// If the json block is not complete, continue reading
			// the json ends in a } with a newline
			if len(jsonBlock) < 2 || jsonBlock[len(jsonBlock)-2:] != "}\n" {
				continue
			}

			c.reader <- jsonBlock

			jsonBlock = ""
		}
	}()

	for {
		select {
		case <-c.close:
			c.Close()
			return
		case message := <-c.reader:
			data := map[string]interface{}{}
			err := json.Unmarshal([]byte(message), &data)

			if err != nil {
				log.Println("Error parsing message", err)
				return
			}

			for _, handler := range c.handlers["message"] {
				handler(data)
			}
		case data := <-c.writer:
			_, err := c.Response.Write(data)

			if err != nil {
				panic(err)
			}

			flusher.Flush()
		}
	}
}

/*
Send a message on the connection.
*/
func (c *Connection) Send(event string, data []byte) []byte {
	responseHash := fmt.Sprintf("%x", sha1.New().Sum([]byte(fmt.Sprintf("%s%s", uuid.New().String(), c.messageHash))))
	responseChan := make(chan []byte)

	c.ResponseCallbacks[responseHash] = func(response []byte) {
		defer delete(c.ResponseCallbacks, responseHash)
		c.RequestCount++
		responseChan <- response
	}

	c.writer <- []byte(c.toJson(responseHash, event, data))

	return <-responseChan
}

/*
The target amount of ulization for the connection that is based on how many
requests it takes to pay for the connection second.
*/
func (c *Connection) targetUtilization() float64 {
	if c.closedAt.IsZero() || c.ConnectedAt.IsZero() {
		return 0
	}

	connectionTime := c.closedAt.Sub(c.ConnectedAt).Seconds()
	targetRequestsPerSecond := 3

	return connectionTime * float64(targetRequestsPerSecond)
}

/*
Encode the connection to a json string.
*/
func (c *Connection) toJson(responseHash, event string, data []byte) string {
	jsonData, err := json.Marshal(map[string]interface{}{
		"databaseUuid":   c.databaseUuid,
		"branchUuid":     c.branchUuid,
		"connectionId":   c.connectionId,
		"connectionHash": c.connectionHash,
		"event":          event,
		"data":           data,
		"messageHash":    c.messageHash,
		"responseHash":   responseHash,
	})

	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s\n", jsonData)
}

/*
Consume credits from the database's request balance when the connection is
underutilized.
*/
func (c *Connection) updateRequestBalance() {
	if c.databaseUuid == "" || c.branchUuid == "" {
		return
	}

	if c.wasFullyUtilized() {
		if connectionBalance := ConnectionBalanceFor(c.databaseUuid, c.branchUuid); connectionBalance != nil {
			connectionBalance.Consume()
		}
	}
}

func (c *Connection) Verify(verificationNonce string) {
	connectionKey, err := auth.SecretsManager().GetConnectionKey(c.databaseUuid, c.branchUuid)

	if err != nil {
		return
	}

	serverNonceHash := hmac.New(sha256.New, []byte(connectionKey))
	serverNonceHash.Write([]byte(fmt.Sprintf("%s:%s", c.connectionHash, c.messageHash)))
	serverNonce := fmt.Sprintf("%x", serverNonceHash.Sum(nil))

	if subtle.ConstantTimeCompare([]byte(serverNonce), []byte(verificationNonce)) != 1 {
		c.close <- true
		return
	}

	c.open = true
}

/*
Determine if the connection was fully utilized.
*/
func (c *Connection) wasFullyUtilized() bool {
	target := c.targetUtilization()

	if target == 0 {
		return false
	}

	return float64(c.RequestCount) < target
}
