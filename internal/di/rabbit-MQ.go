package di

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func (d *DI) GetRmq() *amqp.Connection {
	if d.rabbitMQConn != nil {
		return d.rabbitMQConn
	}

	conn, err := amqp.Dial(d.Config().RabbitMQDSN())
	if err != nil {
		d.logger.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
		return nil
	}

	d.rabbitMQConn = conn

	return conn
}
