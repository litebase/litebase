package http

import (
	"fmt"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/config"
	"log"
	"strconv"
	"time"

	"golang.org/x/exp/slices"
)

type ConnectionManager struct {
	connections       []*Connection
	connectionClient  *Client
	connectionTimer   *time.Ticker
	healthCheckFailed bool
	healthCheckNonces []string
	messageCount      int
}

var staticConnectionManager *ConnectionManager

func SecretsManager() *ConnectionManager {
	if staticConnectionManager == nil {
		staticConnectionManager = &ConnectionManager{
			connections: []*Connection{},
		}
	}

	return staticConnectionManager
}

func (c *ConnectionManager) Close() {
	c.client().Close()
}

func (c *ConnectionManager) client() *Client {
	if c.connectionClient == nil {
		c.connectionClient = &Client{}
		c.connectionClient.Dial()
	}

	return c.connectionClient
}

func (c *ConnectionManager) connect() bool {
	host := c.getHost()
	port := c.getPort()

	databaseUuid := config.Get("database_uuid")
	branchUuid := config.Get("branch_uuid")
	path := fmt.Sprintf("databases/%s/%s/connection", databaseUuid, branchUuid)
	headers := map[string][]string{
		"Content-Type": {"application/json"},
		"Host":         {fmt.Sprintf("%s:%s", host, port)},
		"X-LBDB-Date":  {fmt.Sprintf("%x", time.Now().Unix())},
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
			"GET",
			path,
			headersToSign,
			map[string]string{},
			map[string]string{},
		),
	}

	err = c.client().Open(host, path, headers)

	return err == nil
}

func (c *ConnectionManager) createConnection() *Connection {
	connection := NewConnection(
		c.client(),
		config.Get("database_uuid"),
		config.Get("branch_uuid"),
	)

	c.connections = append(c.connections, connection)

	return connection
}

func (c *ConnectionManager) getHost() string {
	return config.Get("LITEBASEDB_ROUTER_HOST")
}

func (c *ConnectionManager) getPort() string {
	return config.Get("LITEBASEDB_ROUTER_PORT")
}

func (c *ConnectionManager) IncrementMessageCount() int {
	c.messageCount++

	return c.messageCount
}

func (c *ConnectionManager) Listen() {
	connected := c.connect()

	if !connected {
		return
	}

	c.createConnection().Listen()
}

func (c *ConnectionManager) ResetMessageCount() {
	c.messageCount = 0
}

func (c *ConnectionManager) SetHealthCheckResponse(nonce string) {
	if !slices.Contains(c.healthCheckNonces, nonce) {
		c.healthCheckFailed = true
	}

	c.healthCheckNonces = c.healthCheckNonces[len(c.healthCheckNonces)-4:]
}

func (c *ConnectionManager) Tick() {
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
					c.connectionTimer.Stop()
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
