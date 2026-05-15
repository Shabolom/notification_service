package di

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
)

func (d *DI) NewProducer() *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(d.Config().Kafka.Brokers, ","),

		// Аналог RequiredAcks: kafka.RequireOne
		"acks": "1",

		// Аналог таймаутов
		"message.timeout.ms": 10000,
		"socket.timeout.ms":  10000,

		// Аналог automatic topic creation
		"allow.auto.create.topics": true,

		// Аналог LeastBytes прямого нет.
		// В confluent producer partitioning работает иначе.
		"partitioner": "murmur2_random",
	})

	if err != nil {
		panic(err)
	}

	go func() {
		for event := range producer.Events() {
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					d.Logger().Error(e.TopicPartition.Error.Error())
				}
			}
		}
	}()

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
