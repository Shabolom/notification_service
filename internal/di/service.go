package di

import (
	"context"
	"notification_service/internal/service/event"
)

type ExampleService interface {
	Health(ctx context.Context) error
}

func (d *DI) GetEventService() ExampleService {
	return event.New()
}
