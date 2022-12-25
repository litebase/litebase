package http

import (
	"fmt"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/config"
	"log"
	"strconv"
	"time"
)

type ConnectionManagerInstance struct {
	connections      []*Connection
	connectionClient *Client
	connectionTimer  *time.Ticker
	host             string
	messageCount     int
}

var staticConnectionManager *ConnectionManagerInstance

func ConnectionManager() *ConnectionManagerInstance {
	if staticConnectionManager == nil {
		staticConnectionManager = &ConnectionManagerInstance{
			connections: []*Connection{},
		}
	}

	return staticConnectionManager
}

func (c *ConnectionManagerInstance) Close() {
	if c.connectionTimer != nil {
		c.connectionClient.Close()
	}
}

func (c *ConnectionManagerInstance) client() *Client {
	if c.connectionClient == nil || c.connectionClient.Closed {
		c.connectionClient = NewClient(c.host)
	}

	return c.connectionClient
}

func (c *ConnectionManagerInstance) connect(host string) bool {
	c.host = host
	databaseUuid := config.Get("database_uuid")
	branchUuid := config.Get("branch_uuid")
	path := fmt.Sprintf("databases/%s/%s/connection", databaseUuid, branchUuid)

	headers := map[string][]string{
		"Content-Type": {"application/json"},
		"Host":         {host},
		"X-LBDB-Date":  {fmt.Sprintf("%d", time.Now().UTC().Unix())},
	}

	headersToSign := map[string]string{}

	for key, value := range headers {
		headersToSign[key] = value[0]
	}

	connectionKey, err := auth.SecretsManager().GetConnectionKey(
		databaseUuid,
		branchUuid,
	)

	if err != nil {
		log.Fatal(err)
	}

	headers["Authorization"] = []string{
		auth.SignRequest(
			databaseUuid,
			connectionKey,
			"POST",
			path,
			headersToSign,
			map[string]interface{}{},
			map[string]string{},
		),
	}

	err = c.client().Open(host, path, headers)

	return err == nil
}

func (c *ConnectionManagerInstance) createConnection() *Connection {
	connection := NewConnection(
		c.client(),
		config.Get("database_uuid"),
		config.Get("branch_uuid"),
	)

	c.connections = append(c.connections, connection)

	return connection
}

func (c *ConnectionManagerInstance) IncrementMessageCount() int {
	c.messageCount++

	return c.messageCount
}

func (c *ConnectionManagerInstance) Listen(host string) {
	connection := c.createConnection()
	go connection.Listen()
	connected := c.connect(host)

	if !connected {
		return
	}

	c.createConnection().Listen()
}

func (c *ConnectionManagerInstance) ResetMessageCount() {
	c.messageCount = 0
}

func (c *ConnectionManagerInstance) Tick() {
	if c.connectionTimer != nil {
		c.connectionTimer.Stop()
		c.ResetMessageCount()
	}

	count := c.IncrementMessageCount()

	seconds, err := strconv.ParseInt(config.Get("target_connection_time_in_seconds"), 10, 64)

	if err != nil {
		log.Fatal(err)
	}

	c.connectionTimer = time.NewTicker(time.Duration(seconds) * time.Second)

	quit := make(chan bool)

	go func() {
		for {
			select {
			case <-c.connectionTimer.C:
				if count == c.messageCount {
					c.Close()
					quit <- true
				}

				c.ResetMessageCount()
			case <-quit:
				c.connectionTimer.Stop()
				return
			}
		}
	}()
}
