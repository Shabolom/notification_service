package kafkaProducer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

type Kafka struct {
	producer   *kafka.Producer
	serializer *jsonschema.Serializer
	topic      string
}

func NewKafka(producer *kafka.Producer, serializer *jsonschema.Serializer, topic string) *Kafka {
	return &Kafka{
		producer:   producer,
		serializer: serializer,
	}
}
