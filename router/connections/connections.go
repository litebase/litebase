package connections

import (
	"fmt"
	"litebasedb/internal/config"
	"litebasedb/router/auth"
	"litebasedb/router/node"
	"litebasedb/router/runtime"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
)

var Clients = make(map[string]*lambda.Lambda)

/*
The runtime connections map.
*/
var Connections = make(map[string]map[string]*Connection)

/*
The runtime connection balance map.
*/
var ConnectionBalances = make(map[string]*ConnectionBalance)

/*
Connection locks for a database.
*/
var ConnectionLocks = make(map[string]bool)

/*
Return all the connections for a database.
*/
func All(
	databaseUuid string,
	branchUuid string,
) []*Connection {
	return Purge(databaseUuid, branchUuid, Connections[Key(databaseUuid, branchUuid)])
}

/*
Balance the number of connections opened for a database.
*/
func Balance(databaseUuid, branchUuid string) bool {
	requestsPerSecond := RequestsPerSecond(databaseUuid, branchUuid)
	targetConnections := math.Min(float64(requestsPerSecond/60), 100)
	connections := All(databaseUuid, branchUuid)
	connectionsCount := float64(len(connections))
	targetConnectionTimeSeconds, err := strconv.ParseInt(config.Get("target_connection_time_in_seconds"), 10, 64)

	if err != nil {
		targetConnectionTimeSeconds = 60
	}

	if targetConnections == 0 && connectionsCount >= 1 {
		for _, connection := range connections {
			connectionCreationDifference := time.Now().Unix() - connection.ConnectedAt.Unix()

			if !connection.connected || connectionCreationDifference < targetConnectionTimeSeconds {
				continue
			}

			if connection.IsActive() {
				connection.Close()
			}
		}

		return false
	}

	if targetConnections > connectionsCount {
		targetCount := targetConnections - connectionsCount
		log.Printf("%s:%s: Opening %d connections: ", databaseUuid, branchUuid, int(targetCount))

		fn, err := auth.SecretsManager().GetFunctionName(databaseUuid, branchUuid)

		if err != nil {
			return false
		}

		for i := 0; i < int(targetCount); i++ {
			if config.Get("env") == "local" {
				fn = "function"
			}

			RequestConnection(databaseUuid, branchUuid, fn)
		}

		return true
	}

	if targetConnections < connectionsCount {
		targetCount := connectionsCount - targetConnections

		for i := 0; i < int(targetCount); i++ {
			go connections[i].Close()
		}

		return true
	}

	return false
}

func ClientKey(databaseUuid, branchUuid string) string {
	return fmt.Sprintf("%s-%s", databaseUuid, branchUuid)
}

func ConnectionBalanceFor(databaseUuid, branchUuid string) *ConnectionBalance {
	if _, ok := ConnectionBalances[Key(databaseUuid, branchUuid)]; !ok {
		ConnectionBalances[Key(databaseUuid, branchUuid)] = NewConnectionBalance(databaseUuid, branchUuid)
	}

	return ConnectionBalances[Key(databaseUuid, branchUuid)]
}

/*
Create a connection for the database if one does not exist and the database
has a remaining balance for connections created outside of auto scaling.

TODO: Rate-limit: Only allow connections to be created once per second with a backoff policy.
*/
func Create(databaseUuid, branchUuid, fn string) bool {
	if _, ok := ConnectionLocks[Key(databaseUuid, branchUuid)]; ok && ConnectionLocks[Key(databaseUuid, branchUuid)] {
		return false
	}

	connectionBalance := ConnectionBalanceFor(databaseUuid, branchUuid)
	connectionBalance.Tick()

	if connectionBalance.IsNegative() {
		return false
	}

	ConnectionLocks[Key(databaseUuid, branchUuid)] = true

	result, err := RequestConnection(
		databaseUuid,
		branchUuid,
		fn,
	)

	ConnectionLocks[Key(databaseUuid, branchUuid)] = false

	if err != nil {
		log.Println(err)
		return false
	}

	return result
}

func DeleteConnection(databaseUuid, branchUuid, connectionId string) {
	delete(Connections[Key(databaseUuid, branchUuid)], connectionId)
}

func For(databaseUuid, branchUuid string) *Connection {
	if _, ok := Connections[Key(databaseUuid, branchUuid)]; !ok || len(Connections[Key(databaseUuid, branchUuid)]) == 0 {
		return nil
	}

	// Get a random connection
	rand.Seed(time.Now().UnixNano())
	c := rand.Float64()

	i := 0

	activeConnections := []*Connection{}

	for connection := range Connections[Key(databaseUuid, branchUuid)] {
		if !Connections[Key(databaseUuid, branchUuid)][connection].IsActive() {
			continue
		}

		activeConnections = append(activeConnections, Connections[Key(databaseUuid, branchUuid)][connection])
	}

	index := int(math.Ceil(c*float64(len(activeConnections)))) - 1

	for _, connection := range activeConnections {
		if !connection.IsActive() {
			continue
		}

		if i == index {
			return connection
		}

		i++
	}

	return nil
}

func GetCredentials(datbaseUuid, branchUuid string) *auth.AWSCredentials {
	credentials, err := auth.SecretsManager().GetAwsCredentials(datbaseUuid, branchUuid)

	if err != nil {
		return nil
	}

	return credentials
}

func Key(databaseUuid, branchUuid string) string {
	return fmt.Sprintf("%s:%s", databaseUuid, branchUuid)
}

func lambdaClient(databaseUuid, branchUuid string) *lambda.Lambda {
	clientKey := ClientKey(databaseUuid, branchUuid)

	if _, ok := Clients[clientKey]; !ok {
		awsCredentials := GetCredentials(databaseUuid, branchUuid)

		if awsCredentials == nil {
			return nil
		}

		var endpoint string

		if config.Get("env") == "local" {
			endpoint = "http://127.0.0.1:8001"
		}

		awsSession, err := session.NewSession(aws.NewConfig().WithRegion(config.Get("region")).WithCredentials(credentials.NewStaticCredentials(
			awsCredentials.Key,
			awsCredentials.Secret,
			awsCredentials.Token,
		)).WithEndpoint(endpoint))

		if err != nil {
			log.Fatal(err)
		}

		Clients[clientKey] = lambda.New(awsSession)
	}

	return Clients[clientKey]
}

func Purge(
	databaseUuid string,
	branchUuid string,
	connections map[string]*Connection,
) []*Connection {
	var open []*Connection

	for _, connection := range connections {
		if connection.IsDraining() {
			continue
		} else if connection.IsOpen() {
			open = append(open, connection)
		} else if connection.IsClosed() {
			delete(connections, connection.connectionId)
		}
	}

	return open
}

func PutConnection(databaseUuid, branchUuid string, connection *Connection) {
	if _, ok := Connections[Key(databaseUuid, branchUuid)]; !ok {
		Connections[Key(databaseUuid, branchUuid)] = map[string]*Connection{}
	}

	Connections[Key(databaseUuid, branchUuid)][connection.connectionId] = connection
}

/*
Create a connection to the Runtime.
*/
func RequestConnection(databaseUuid, branchUuid, fn string) (bool, error) {
	// TODO: Need to lock this for a small period of time to prevent multiple connections from being created at the same time.
	client := lambdaClient(databaseUuid, branchUuid)

	if client == nil {
		return false, nil
	}

	payload, err := runtime.PrepareRequest(&runtime.RuntimeRequestObject{
		AccessKeyId: "",
		Body: map[string]interface{}{
			"host": node.GetIPv6Address(),
		},
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		Headers: map[string]string{
			"content-type": "application/json",
			"host":         "localhost",
			"x-lbdb-date":  fmt.Sprintf("%d", time.Now().UTC().Unix()),
		},
		Host:   "localhost",
		Method: "POST",
		Path:   "/connection",
		Query:  map[string]string{},
	}, true)

	if err != nil {
		return false, err
	}

	_, err = client.Invoke(&lambda.InvokeInput{
		FunctionName:   aws.String(fn),
		InvocationType: aws.String("Event"),
		Payload:        payload,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == lambda.ErrCodeServiceException {
				log.Println(lambda.ErrCodeServiceException, aerr.Error())

				Clients[ClientKey(databaseUuid, branchUuid)] = nil
			}
		}

		return false, err
	}

	return true, nil
}

func Send(databaseUuid, branchUuid, fn string, payload []byte) []byte {
	client := lambdaClient(databaseUuid, branchUuid)

	if client == nil {
		return nil
	}

	response, err := client.Invoke(&lambda.InvokeInput{
		FunctionName:   aws.String(fn),
		InvocationType: aws.String("RequestResponse"),
		Payload:        payload,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == lambda.ErrCodeServiceException {
				log.Println(lambda.ErrCodeServiceException, aerr.Error())

				Clients[ClientKey(databaseUuid, branchUuid)] = nil
			}
		}

		return nil
	}

	if aws.Int64Value(response.StatusCode) >= int64(400) {
		return nil
	}

	return response.Payload
}

func SendThroughConnection(databaseUuid, branchUuid, fn string, payload []byte) []byte {
	connection := For(databaseUuid, branchUuid)

	if connection == nil && Create(databaseUuid, branchUuid, fn) {
		connection = For(databaseUuid, branchUuid)
	}

	if connection == nil {
		return nil
	}

	if !connection.IsActive() {
		return nil
	}

	return connection.Send("QUERY", payload, true)
}
