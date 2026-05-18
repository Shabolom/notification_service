package rabbit

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Rabbit struct {
	conn   *amqp.Connection
	logger *zap.Logger
	ctx    context.Context
	ch     *amqp.Channel
}

func New(ctx context.Context, conn *amqp.Connection, logger *zap.Logger) *Rabbit {
	return &Rabbit{
		conn:   conn,
		logger: logger,
		ctx:    ctx,
	}
}
