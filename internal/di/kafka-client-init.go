package di

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"go.uber.org/zap"
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
		"allow.auto.create.topics": false,

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
				topic := ""
				if e.TopicPartition.Topic != nil {
					topic = *e.TopicPartition.Topic
				}

				if e.TopicPartition.Error != nil {
					d.logger.Error(
						"delivery failed",
						zap.String("topic", topic),
						zap.Int32("partition", e.TopicPartition.Partition),
						zap.Error(e.TopicPartition.Error),
					)
				} else {
					d.logger.Info(
						"message delivered",
						zap.String("topic", topic),
						zap.Int32("partition", e.TopicPartition.Partition),
						zap.Int64("offset", int64(e.TopicPartition.Offset)),
					)
				}

			case kafka.Error:
				d.logger.Error(
					"kafka client error",
					zap.Error(e),
				)
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
