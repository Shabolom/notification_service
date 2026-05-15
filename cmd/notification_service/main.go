package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"notification_service/internal/di"

	"github.com/joho/godotenv"
)

func main() {
	ctx := context.Background()

	if err := godotenv.Load("./build/local/.env"); err != nil {
		panic(err)
	}

	container := di.New(ctx)
	container.Logger()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	container.GetEventService().Exec()
	container.Logger().Info("Exec started")

	<-stop
	
	container.Logger().Info("Shutting down...")

	container.ShotDown()

	container.Logger().Info("Shut down complete")
}
