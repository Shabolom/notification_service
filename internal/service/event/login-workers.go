package event

import (
	"notification_service/internal/dto"
	"time"

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

			s.handleLoginMessage(msg)
		}
	}
}

func (s *Service) handleLoginMessage(msg amqp.Delivery) {
	if msg.ContentType != amqp.MimeTextPlain {
		s.logger.Info("msg ContentType is not TEXTTYPE", zap.Any("message id", msg.MessageId))
		_ = msg.Nack(false, false)
		return
	}

	<-s.tokenChLimiter

	event := &dto.Event{
		Time:  time.Now(),
		Email: string(msg.Body),
		Type:  "login",
	}

	if err := s.notificator.WriteNotificationLogin(event.Email, "login"); err != nil {
		s.logger.Fatal("failed to send email", zap.Error(err))
		_ = msg.Nack(false, false)
		return
	}

	if err := s.kafkaProducer.WriteEvent(event); err != nil {
		s.logger.Fatal("failed to send a login event to kafka", zap.Any("event", event), zap.Error(err))
		_ = msg.Nack(false, false)
		return
	}

	err := msg.Ack(false)
	if err != nil {
		s.logger.Warn("failed to ack the message", zap.Error(err))
	}
}
