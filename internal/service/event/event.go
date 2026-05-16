package event

import (
	"context"
	"notification_service/internal/dto"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Notificator interface {
	WriteNotificationRegister(email string, action string) error
	WriteNotificationLogin(email string, action string) error
}

type KafkaProducer interface {
	WriteEvent(event *dto.Event) error
}

type Rabbit interface {
	GettingConsumers() (error error, loginConsumer <-chan amqp.Delivery, registerConsumer <-chan amqp.Delivery)
}
type Service struct {
	wg     *sync.WaitGroup
	logger *zap.Logger
	ctx    context.Context

	kafkaProducer KafkaProducer

	notificator Notificator

	rabbit Rabbit

	tokenChLimiter chan struct{}
}

func New(
	wg *sync.WaitGroup,
	logger *zap.Logger,
	ctx context.Context,
	kafkaProducer KafkaProducer,
	notificator Notificator,
	rabbit Rabbit,
) *Service {
	return &Service{
		wg:             wg,
		logger:         logger,
		ctx:            ctx,
		kafkaProducer:  kafkaProducer,
		notificator:    notificator,
		rabbit:         rabbit,
		tokenChLimiter: make(chan struct{}, 10),
	}
}

const (
	RATE         = 5
	MESSAGECOUNT = 10
)
