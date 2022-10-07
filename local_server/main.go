package main

import (
	"encoding/json"
	"litebasedb/runtime/app"
	"litebasedb/runtime/app/event"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func init() {
	os.Setenv("LITEBASEDB_RUNTIME_ID", uuid.NewString())
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	godotenv.Load()
}

func main() {
	server := fiber.New()

	var handler = app.NewApp().Handler

	server.Post(
		"/2015-03-31/functions/:function/invocations",
		func(c *fiber.Ctx) error {
			request := event.Event{}
			json.Unmarshal([]byte(c.Body()), &request)
			response := handler(request)
			json, _ := json.Marshal(response)

			return c.Send(json)
		})

	log.Fatal(server.Listen(":8001"))
}
