package event

import (
	"context"
	"notification_service/internal/dto"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
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

			s.handleRegisterMessage(msg)
		}
	}
}

func (s *Service) handleRegisterMessage(msg amqp.Delivery) {
	if msg.ContentType != amqp.MimeTextPlain {
		s.logger.Info("msg ContentType is not TEXTTYPE", zap.Any("message id", msg.MessageId))
		_ = msg.Nack(false, false)
		return
	}

	<-s.tokenChLimiter

	event := &dto.Event{
		Time:  time.Now(),
		Email: string(msg.Body),
		Type:  "register",
	}

	if err := s.notificator.WriteNotification(event.Email); err != nil {
		s.logger.Warn("failed to send email", zap.Error(err))
		_ = msg.Nack(false, true)
		return
	}

	msgCtx, cancel := context.WithTimeout(s.ctx, time.Second*3)
	defer cancel()

	if err := s.kafkaProducer.WriteEvent(msgCtx, event); err != nil {
		s.logger.Warn("failed to send a login event to kafka", zap.Any("event", event), zap.Error(err))
		_ = msg.Nack(false, true)
		return
	}

	if err := msg.Ack(false); err != nil {
		s.logger.Warn("failed to ack the message", zap.Error(err))
	}
}
