package main

import (
	"errors"
	"fmt"
	"litebasedb/router/auth"
	"litebasedb/router/http"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
)

type App struct {
	// intervals map[string]time.Timer
	server *fiber.App
}

func NewApp() *App {
	app := &App{}
	auth.SecretsManager().Init()
	auth.KeyManagerInit()

	return app
}

func (app *App) Serve() {
	app.server = fiber.New(fiber.Config{
		IdleTimeout:  60 * time.Second,
		ErrorHandler: app.ErrorHandler,
	})

	// app.server.Use(recover.New())

	http.Routes(app.server)

	app.server.Listen(fmt.Sprintf(`:%s`, os.Getenv("LITEBASEDB_PORT")))
}

func (app *App) ErrorHandler(ctx *fiber.Ctx, err error) error {
	var e *fiber.Error
	code := fiber.StatusInternalServerError

	if errors.As(err, &e) {
		code = e.Code
	}

	err = ctx.Status(code).SendString("")

	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).SendString("Internal Server Error")
	}

	return nil
}
