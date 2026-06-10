package event

import (
	"context"
	"errors"
	"notification_service/internal/dto"
	"notification_service/pkg/utils"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	LoginQueueName         = "auth.login.logs"
	RegisterQueueName      = "auth.register"
	LoginQueueRetryName    = "auth.login.logs.retry"
	RegisterQueueRetryName = "auth.register.retry"
	LoginQueueDLQKey       = "login.logs.dlq"
	RegisterQueueDLQKey    = "register.dlq"
)

func (s *Service) registerWorkersStart(ch <-chan amqp.Delivery) {
	for i := 0; i < MESSAGECOUNT; i++ {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()
			s.registerWorkers(ch)
		}()
	}
}

func (s *Service) registerWorkers(ch <-chan amqp.Delivery) {
	for {
		select {
		case <-s.ctx.Done():
			return

		case msg, ok := <-ch:
			if !ok {
				s.logger.Warn("channel was closed")
				return
			}

			s.handleRegisterMessage(&msg)
		}
	}
}

func (s *Service) handleRegisterMessage(msg *amqp.Delivery) {
	publishToDLQ := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		return s.rabbit.PublishToDLQ(ctx, msg, RegisterQueueDLQKey)
	}

	if msg.ContentType != amqp.MimeTextPlain {
		s.logger.Warn(
			"invalid message content type",
			zap.String("message_id", msg.MessageId),
			zap.String("content_type", msg.ContentType),
		)

		if err := publishToDLQ(); err != nil {
			s.logger.Warn(
				"failed to publish invalid content type message to DLQ",
				zap.String("message_id", msg.MessageId),
				zap.String("content_type", msg.ContentType),
				zap.Error(err),
			)

			_ = msg.Nack(false, true)
			return
		}

		_ = msg.Ack(false)
		return
	}

	select {
	case <-s.ctx.Done():
		_ = msg.Nack(false, true)
		return
	case <-s.tokenChLimiter:
	}

	event := &dto.Event{
		Time:  time.Now(),
		Email: string(msg.Body),
		Type:  "register",
	}

	err := s.kafkaProducer.WriteEvent(event)
	if err != nil {
		s.logger.Error("failed to send event to kafka", zap.Error(err))

		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.IsRetriable() {
			if utils.ReachedRetryLimit(msg, RegisterQueueRetryName, 5) {
				if err := publishToDLQ(); err != nil {
					s.logger.Warn("failed to publish msg to DLQ", zap.Error(err))
					_ = msg.Nack(false, true)
					return
				}

				_ = msg.Ack(false)
				return
			}

			_ = msg.Nack(false, false)
			return
		}

		if err := publishToDLQ(); err != nil {
			s.logger.Warn("failed to publish msg to DLQ", zap.Error(err))
			_ = msg.Nack(false, true)
			return
		}

		_ = msg.Ack(false)
		return
	}

	if err = s.notificator.WriteNotificationRegister(event.Email, "register"); err != nil {
		s.logger.Warn("failed to send register notification", zap.Error(err))

		if utils.ReachedRetryLimit(msg, RegisterQueueRetryName, 5) {
			if err := publishToDLQ(); err != nil {
				s.logger.Warn("failed to publish msg to DLQ", zap.Error(err))
				_ = msg.Nack(false, true)
				return
			}

			_ = msg.Ack(false)
			return
		}

		_ = msg.Nack(false, false)
		return
	}

	_ = msg.Ack(false)
}
