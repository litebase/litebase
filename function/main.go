package main

import (
	"context"
	"litebasedb/runtime/app"
	"litebasedb/runtime/app/event"
	"litebasedb/runtime/app/http"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

var handler = app.Handler

func HandleRequest(ctx context.Context, data *event.Event) (*http.Response, error) {
	return handler(data), nil
}

func init() {
	os.Setenv("LITEBASEDB_RUNTIME_ID", uuid.NewString())
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	godotenv.Load()
}

func main() {
	lambda.Start(HandleRequest)
}
