package main

import (
	"litebase/storage"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	s := storage.New()
	s.Init()

	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		log.Println("Running in Lambda environment")
		lambda.Start(httpadapter.New(http.DefaultServeMux).ProxyWithContext)
	} else {
		s.Serve()
	}
}
