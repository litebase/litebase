package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/app/event"
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

func (c *Connection) handleQuery(data string) {
	config.Set("database_uuid", c.databaseUuid)
	e := &event.Event{}
	json.Unmarshal([]byte(data), e)
	result := Router().Dispatch(e)

	response := map[string]interface{}{
		"connectionId":   c.connectionId,
		"connectionHash": c.connectionHash,
		"event":          "QUERY_RESPONSE",
		"response":       result,
		// "responseHash":   e.Body["responseHash"],
	}

	jsonResponse, err := json.Marshal(response)

	if err != nil {
		log.Fatal(err)
	}

	c.client.Send(string(jsonResponse))
}

func (c *Connection) handleHealthCheckResponse(data interface{}) bool {
	if data.(map[string]interface{})["nonce"] != nil {
		SecretsManager().SetHealthCheckResponse(data.(map[string]string)["nonce"])
	}
	return true
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
		case message := <-c.client.Messages:
			// c.writer.Write([]byte(message))
			data := map[string]string{}
			json.Unmarshal([]byte(message), &data)
			if c.connectionId == "" && c.connectionHash == "" && data["connectionId"] != "" && data["connectionHash"] != "" {
				c.connectionId = data["connectionId"]
				c.connectionHash = data["connectionHash"]
				// c.setQueryHandler()
			}

			switch data["event"] {
			case "CONNECTION_OPEN":
				c.authenticate()
			case "CONNECTION_READY":
				response := c.handleReady(data["messageHash"])

				if !response {
					c.client.Close()
				}
			case "HEALTH_CHECK_RESPONSE":
				response := c.handleHealthCheckResponse(data)

				if !response {
					c.client.Close()
				}
			case "QUERY":
				// c.handleQuery(data)
			default:
				fmt.Println("Unknown event", data["event"])
			}

			if data["event"] == "QUERY" {
				SecretsManager().Tick()
			}

		case <-c.client.End:
			fmt.Println("Connection closed")
			return
		}
	}
}
