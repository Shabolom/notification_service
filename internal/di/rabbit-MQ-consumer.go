package di

import (
	"notification_service/internal/rabbit"
)

func (d *DI) GetRabbitMQConsumer() *rabbit.Rabbit {
	return rabbit.New(d.ctx, d.GetRmq(), d.Logger())
}
