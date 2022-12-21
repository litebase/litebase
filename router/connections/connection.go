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
	"sync"
	"time"

	"github.com/google/uuid"
)

var i = 0

type Connection struct {
	branchUuid           string
	close                chan bool
	closing              bool
	closedAt             time.Time
	connected            bool
	ConnectedAt          time.Time
	connectionHash       string
	connectionId         string
	databaseUuid         string
	drained              chan bool
	draining             bool
	handlers             map[string][]func(map[string]interface{})
	messageHash          []byte
	mutex                *sync.Mutex
	open                 bool
	reader               chan string
	Request              *http.Request
	RequestCount         int
	ResponseCount        int
	requestInFlightCount int
	Response             http.ResponseWriter
	responses            map[string]chan []byte
	writer               chan []byte
}

func NewConnection(connectionId string, connectionHash string, request *http.Request, w http.ResponseWriter) *Connection {
	return &Connection{
		close:          make(chan bool),
		connected:      false,
		connectionHash: connectionHash,
		connectionId:   connectionId,
		drained:        make(chan bool),
		handlers:       make(map[string][]func(map[string]interface{})),
		messageHash:    nil,
		mutex:          &sync.Mutex{},
		open:           false,
		reader:         make(chan string),
		Request:        request,
		RequestCount:   0,
		ResponseCount:  0,
		Response:       w,
		responses:      make(map[string]chan []byte),
		writer:         make(chan []byte),
	}
}

func CreateConnection(databaseUuid, branchUuid, connectionKey string, request *http.Request, w http.ResponseWriter) *Connection {
	connectionId := fmt.Sprintf("%s", sha1.New().Sum([]byte(uuid.New().String())))
	connectionHash := fmt.Sprintf("%s", hmac.New(sha256.New, []byte(connectionKey)).Sum([]byte(connectionId)))
	connection := NewConnection(string(connectionId), string(connectionHash), request, w)
	PutConnection("unassigned", "", connection)
	go connection.Run()

	return connection
}

func (c *Connection) Authenticate(databaseuuid, branchUuid string) {
	c.messageHash = sha256.New().Sum([]byte(fmt.Sprintf("%s%s", uuid.New().String(), c.connectionId)))
	c.databaseUuid = databaseuuid
	c.branchUuid = branchUuid

	DeleteConnection("unassigned", "", c.connectionId)
	PutConnection(databaseuuid, branchUuid, c)

	c.Send("CONNECTION_READY", nil, false)
}

/*
Close the connection.
*/
func (c *Connection) Close() bool {
	if c.closing || c.RequestCount != c.ResponseCount {
		return false
	}

	c.Send("CONNECTION_CLOSED", nil, false)

	c.closing = true
	c.close <- true
	c.open = false

	c.closedAt = time.Now()

	close(c.close)
	close(c.reader)
	close(c.writer)

	c.updateRequestBalance()

	if c.databaseUuid != "" && c.branchUuid != "" {
		DeleteConnection(c.databaseUuid, c.branchUuid, c.connectionId)
	} else {
		DeleteConnection("unassigned", "", c.connectionId)
	}

	return true
}

/*
Return if the connection is in an active state, and able to process requests. This meaning it is open and not draining.
*/
func (c *Connection) IsActive() bool {
	c.mutex.Lock()
	isActive := !c.ConnectedAt.IsZero() && c.IsOpen() && !c.IsDraining()
	c.mutex.Unlock()

	return isActive

}

/*
Check if the connection is closed. A connection is considered closed if it has been closed and there are no requests in flight.
*/
func (c *Connection) IsClosed() bool {
	return !c.IsDraining() && !c.closedAt.IsZero()
}

/*
*
A connection is considered draining if it has been closed but there are still
requests in flight.
*/
func (c *Connection) IsDraining() bool {
	return c.draining
}

/*
Check if the connection is open. A connection is truly open once it has been
authenticated. However, it is possible to close a connection before it has
been authenticated so we need to check if the connection has been
explicitly closed.
*/
func (c *Connection) IsOpen() bool {
	return c.open || (!c.open && c.closedAt.IsZero())
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
				jsonString, err := json.Marshal(data["response"])

				if err != nil {
					c.responses[data["responseHash"].(string)] <- nil
					return
				}

				c.responses[data["responseHash"].(string)] <- jsonString
			}(data)
		}
	})

	c.On("close", func(data map[string]interface{}) {

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
	c.Send("CONNECTION_OPEN", nil, false)
}

func (c *Connection) Run() {
	c.Response.Header().Set("Content-Type", "text/plain")
	c.Response.Header().Set("Transfer-Encoding", "chunked")

	flusher := c.Response.(http.Flusher)

	go c.onCreated()

	go func() {
		buf := make([]byte, 1024)
		jsonBlock := ""

		for {
			// if the http request is closed, close the connection
			if c.Request.Body == nil {
				c.Close()
				break
			}

			n, err := c.Request.Body.Read(buf)

			if err != nil {
				c.Close()
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
			for _, handler := range c.handlers["close"] {
				handler(map[string]interface{}{})
			}
			return
		case message := <-c.reader:
			c.requestInFlightCount--

			if c.requestInFlightCount <= 0 {
				// c.requestInFlightCount = 0
				// c.drained <- true
			}

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
				c.Close()
			}

			flusher.Flush()
		}
	}
}

/*
Send a message on the connection.
*/
func (c *Connection) Send(event string, data []byte, hasResponse bool) []byte {
	responseHash := fmt.Sprintf("%x", sha1.New().Sum([]byte(fmt.Sprintf("%s%s", uuid.New().String(), c.messageHash))))

	if hasResponse {
		c.responses[responseHash] = make(chan []byte)
		c.RequestCount++
	}

	c.writer <- []byte(c.toJson(responseHash, event, data))

	if hasResponse {
		response := <-c.responses[responseHash]
		c.ResponseCount++

		return response
	}

	return nil
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
		c.Close()
		return
	}

	serverNonceHash := hmac.New(sha256.New, []byte(connectionKey))
	serverNonceHash.Write([]byte(fmt.Sprintf("%s:%s", c.connectionHash, c.messageHash)))
	serverNonce := fmt.Sprintf("%x", serverNonceHash.Sum(nil))

	if subtle.ConstantTimeCompare([]byte(serverNonce), []byte(verificationNonce)) != 1 {
		c.Close()
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
