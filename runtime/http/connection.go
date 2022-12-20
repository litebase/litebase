package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/config"
	"litebasedb/runtime/event"
	"log"
)

type Connection struct {
	client         *Client
	connectionHash string
	connectionId   string
	databaseUuid   string
	branchUuid     string
	messageHash    string
}

func NewConnection(client *Client, databaseUuid string, branchUuid string) *Connection {
	return &Connection{
		client:       client,
		databaseUuid: databaseUuid,
		branchUuid:   branchUuid,
	}
}

func (c *Connection) authenticate() bool {
	json, err := json.Marshal(map[string]string{
		"connectionId":   c.connectionId,
		"connectionHash": c.connectionHash,
		"branchUuid":     c.branchUuid,
		"databaseUuid":   c.databaseUuid,
		"event":          "CONNECTION_AUTH",
	})

	if err != nil {
		return false
	}

	c.client.Send(string(json))

	return true
}

func (c *Connection) handleQuery(data map[string]interface{}) {
	config.Set("database_uuid", c.databaseUuid)
	eventData, err := base64.StdEncoding.DecodeString(data["data"].(string))

	if err != nil {
		log.Fatal(err)
	}

	e := &event.Event{}
	err = json.Unmarshal([]byte(eventData), e)

	if err != nil {
		log.Fatal(err)
	}

	result := Router().Dispatch(e)

	response := map[string]interface{}{
		"connectionId":   c.connectionId,
		"connectionHash": c.connectionHash,
		"event":          "QUERY_RESPONSE",
		"response":       result,
		"responseHash":   data["responseHash"],
	}

	jsonResponse, err := json.Marshal(response)

	if err != nil {
		log.Fatal(err)
	}

	c.client.Send(string(jsonResponse))
}

func (c *Connection) handleReady(messageHash string) bool {
	c.messageHash = messageHash
	nonceHash := hmac.New(sha256.New, []byte(fmt.Sprintf("%s:%s", c.connectionHash, c.messageHash)))
	connectionKey, err := auth.SecretsManager().GetConnectionKey(c.databaseUuid, c.branchUuid)

	if err != nil {
		return false
	}
	nonceHash.Write([]byte(connectionKey))
	nonce := fmt.Sprintf("%x", nonceHash.Sum(nil))

	message := map[string]string{
		"branchUuid":        c.branchUuid,
		"connectionHash":    c.connectionHash,
		"connectionId":      c.connectionId,
		"databaseUuid":      c.databaseUuid,
		"event":             "CONNECTION_VERIFY",
		"verificationNonce": nonce,
	}

	jsonMessage, err := json.Marshal(message)

	if err != nil {
		return false
	}

	c.client.Send(string(jsonMessage))

	return true
}

func (c *Connection) Listen() {
	for {
		select {
		case message := <-c.client.read:
			data := map[string]interface{}{}
			err := json.Unmarshal([]byte(message), &data)

			if message == "" {
				return
			}

			if err != nil {
				c.client.Close()
				return
			}

			if c.connectionId == "" && c.connectionHash == "" && data["connectionId"] != "" && data["connectionHash"] != "" {
				c.connectionId = data["connectionId"].(string)
				c.connectionHash = data["connectionHash"].(string)
			}

			switch data["event"] {
			case "CONNECTION_OPEN":
				c.authenticate()
			case "CONNECTION_READY":
				response := c.handleReady(data["messageHash"].(string))

				if !response {
					c.client.Close()
				}
			case "QUERY":
				c.handleQuery(data)
			default:
				fmt.Println("Unknown event", data["event"])
			}

			if data["event"] == "QUERY" {
				ConnectionManager().Tick()
			}
		}
	}
}
