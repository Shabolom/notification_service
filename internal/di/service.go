package di

import (
	"notification_service/internal/service/event"
)

func (d *DI) GetEventService() *event.Service {
	return event.New(d.wg, d.Logger(), d.ctx, d.GetKafkaProducer(), d.GetNotificator(), d.GetRabbitMQConsumer())
}
