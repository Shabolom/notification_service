package kafkaProducer

import (
	"notification_service/internal/dto"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func (k *Kafka) WriteEvent(event *dto.Event) error {
	payload, err := k.serializer.Serialize(
		k.topic,
		event,
	)
	if err != nil {
		return err
	}

	kafkaMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &k.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	}

	err = k.producer.Produce(kafkaMessage, nil)
	if err != nil {
		return err
	}

	return nil
}
