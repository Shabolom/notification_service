package kafkaProducer

import (
	"context"
	"notification_service/internal/dto"

	"github.com/segmentio/kafka-go"
)

func (k *Kafka) WriteEvent(ctx context.Context, event *dto.Event) error {
	payload, err := k.serializer.Serialize(
		k.producer.Topic,
		&event,
	)
	if err != nil {
		return err
	}

	err = k.producer.WriteMessages(ctx, kafka.Message{
		Value: payload,
	})
	if err != nil {
		return err
	}

	return nil
}
