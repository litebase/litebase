package main

import (
	"encoding/json"
	"litebasedb/runtime"
	"litebasedb/runtime/event"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func init() {
	os.Setenv("LITEBASEDB_RUNTIME_ID", uuid.NewString())
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	server := fiber.New()

	var handler = runtime.Handler

	server.Post(
		"/2015-03-31/functions/:function/invocations",
		func(c *fiber.Ctx) error {
			request := &event.Event{}
			json.Unmarshal([]byte(c.Body()), &request)
			response := handler(request)
			json, _ := json.Marshal(response)

			return c.Send(json)
		})

	log.Fatal(server.Listen(":8001"))
}
