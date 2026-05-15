package di

import (
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

func (d *DI) NewProducer() *kafka.Writer {

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(d.Config().Kafka.Brokers...),
		Topic:                  d.Config().Kafka.Topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		WriteTimeout:           10 * time.Second,
		ReadTimeout:            10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
	}

	return producer
}

func (d *DI) NewKafkaJSONSerializer() *jsonschema.Serializer {
	srClient, err := schemaregistry.NewClient(
		schemaregistry.NewConfig(d.Config().SchemaRegisterDSN()),
	)
	if err != nil {
		d.logger.Fatal("failed to create schema registry client")
	}

	cfg := jsonschema.NewSerializerConfig()
	cfg.AutoRegisterSchemas = false
	cfg.UseLatestVersion = true

	serializer, err := jsonschema.NewSerializer(
		srClient,
		serde.ValueSerde,
		cfg,
	)
	if err != nil {
		d.logger.Fatal("failed to create kafka json schema serializer")
	}

	return serializer
}
