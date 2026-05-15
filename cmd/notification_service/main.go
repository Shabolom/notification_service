package main

import (
	"context"
	"os"

	"notification_service/internal/di"

	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
)

func main() {
	ctx := context.Background()

	if err := godotenv.Load("./build/local/.env"); err != nil {
		panic(err)
	}

	e := echo.New()

	container := di.New(ctx)
	container.Logger()

	// TODO: register routes here
	// handlers := container.GetHTTPHandlers()
	// _ = handlers

	if err := e.Start(":" + os.Getenv("APP_PORT")); err != nil {
		panic(err)
	}
}
