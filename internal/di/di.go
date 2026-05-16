package di

import (
	"context"
	"fmt"
	"notification_service/internal/config"
	kafkaProducer "notification_service/internal/kafka-producer"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/resend/resend-go/v2"
	"go.uber.org/zap"
)

type DI struct {
	config *config.Config
	logger *zap.Logger

	rabbitMQConn *amqp.Connection

	notificator *resend.Client

	ctx context.Context
	wg  *sync.WaitGroup

	kafkaProducer *kafkaProducer.Kafka
}

func New(ctx context.Context) *DI {
	wg := &sync.WaitGroup{}
	return &DI{
		ctx: ctx,
		wg:  wg,
	}
}

func (d *DI) Config() *config.Config {
	if d.config != nil {
		return d.config
	}

	cfg, err := config.FromEnv()
	if err != nil {
		panic(fmt.Errorf("config from env: %w", err))
	}

	d.config = cfg
	return d.config
}

func (d *DI) Logger() *zap.Logger {
	if d.logger != nil {
		return d.logger
	}

	var logger *zap.Logger
	var err error

	if d.Config().Debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}

	if err != nil {
		panic(fmt.Errorf("create logger: %w", err))
	}

	logger = logger.With(
		zap.String("service", d.Config().ServiceName),
		zap.Bool("debug", d.Config().Debug),
	)

	d.logger = logger
	_ = zap.ReplaceGlobals(logger)

	return d.logger
}

func (d *DI) ShotDown() {
	log := d.Logger()
	d.wg.Wait()

	if d.rabbitMQConn != nil {
		if err := d.rabbitMQConn.Close(); err != nil {
			log.Error("failed to close RabbitMQ connection", zap.Error(err))
		} else {
			log.Info("RabbitMQ connection was shut down")
		}
	}

	if d.kafkaProducer != nil {
		d.kafkaProducer.Close()
		log.Info("Kafka producer was shut down")
	}

	if d.logger != nil {
		_ = d.logger.Sync()
		d.logger.Info("logger was shut down")
	}
}
