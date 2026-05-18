package event

import (
	"errors"
	"notification_service/internal/dto"
	"notification_service/pkg/utils"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func (s *Service) loginWorkersStart(ch <-chan amqp.Delivery) {
	for i := 0; i < MESSAGECOUNT; i++ {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()
			s.loginWorkers(ch)
		}()
	}
}

func (s *Service) loginWorkers(ch <-chan amqp.Delivery) {
	for {
		select {
		case <-s.ctx.Done():
			return

		case msg, ok := <-ch:
			if !ok {
				s.logger.Warn("channel was closed")
				return
			}

			s.handleLoginMessage(&msg)
		}
	}
}

func (s *Service) handleLoginMessage(msg *amqp.Delivery) {
	if msg.ContentType != amqp.MimeTextPlain {
		s.logger.Info("msg ContentType is not TEXTTYPE", zap.Any("message id", msg.MessageId))

		err := s.rabbit.PublishToDLQ(msg, LoginQueueDLQKey)
		if err != nil {
			s.logger.Warn(
				"msg ContentType is not TEXTTYPE",
				zap.String("message_id", msg.MessageId),
				zap.String("content_type", msg.ContentType),
			)
			_ = msg.Nack(false, true)
			return
		}

		_ = msg.Ack(false)
		return
	}

	<-s.tokenChLimiter

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
			if utils.ReachedRetryLimit(msg, LoginQueueRetryName, 5) {
				err := s.rabbit.PublishToDLQ(msg, LoginQueueDLQKey)
				if err != nil {
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

		err := s.rabbit.PublishToDLQ(msg, LoginQueueDLQKey)
		if err != nil {
			s.logger.Warn("failed to publish msg to DLQ", zap.Error(err))
			_ = msg.Nack(false, true)
			return
		}

		_ = msg.Ack(false)
		return
	}

	if utils.ReachedRetryLimit(msg, LoginQueueRetryName, 5) {
		err := s.rabbit.PublishToDLQ(msg, LoginQueueDLQKey)
		if err != nil {
			s.logger.Warn("failed to publish msg to DLQ", zap.Error(err))
			_ = msg.Nack(false, true)
			return
		}

		_ = msg.Ack(false)
		return
	}

	if err = s.notificator.WriteNotificationLogin(event.Email, "login"); err != nil {
		s.logger.Warn("failed to send email", zap.Error(err))
		_ = msg.Nack(false, false)
		return
	}

	_ = msg.Ack(false)
}
