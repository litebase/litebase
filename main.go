package main

import (
	"context"
	"litebasedb/runtime"
	"litebasedb/runtime/event"
	"litebasedb/runtime/http"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func init() {
	os.Setenv("LITEBASEDB_RUNTIME_ID", uuid.NewString())
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	godotenv.Load()
}

func main() {
	var handler = runtime.Handler

	lambda.Start(func(context context.Context, event *event.Event) (*http.Response, error) {
		return handler(event), nil
	})
}
