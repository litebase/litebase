package main

import (
	"context"
	"litebasedb/storage"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
)

func HandleRequest(ctx context.Context, event *storage.Event) (string, error) {
	if event.Action == "ping" {
		return "pong", nil
	}

	if event.Action == "create_connection" {
		// Create a new connection
		storage.CreateConnection(
			event.ConnectionUrl,
			event.ConnectionId,
		)
		log.Println("Connection closed...")

		return "", nil
	}

	return "", nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		log.Println("Running in Lambda environment")
		// lambda.Start(HandleRequest)
		lambda.Start(httpadapter.New(http.DefaultServeMux).ProxyWithContext)

	} else {
		s := &storage.Storage{}
		s.Init()
		s.Serve()
	}
}
