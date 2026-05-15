package di

import (
	"context"
	"fmt"
	"notification_service/internal/config"
	kafkaProducer "notification_service/internal/kafka-producer"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/resend/resend-go/v2"
	"go.uber.org/zap"
)

type DI struct {
	config *config.Config
	logger *zap.Logger

	rabbitMQConn *amqp.Connection

	notificator *resend.Client

	ctx           context.Context
	pgConn        *pgxpool.Pool
	kafkaProducer *kafkaProducer.Kafka
}

func New(ctx context.Context) *DI {
	return &DI{
		ctx: ctx,
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
