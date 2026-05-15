package kafkaProducer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	producer   *kafka.Writer
	serializer *jsonschema.Serializer
}

func NewKafka(producer *kafka.Writer, serializer *jsonschema.Serializer) *Kafka {
	return &Kafka{
		producer:   producer,
		serializer: serializer,
	}
}
