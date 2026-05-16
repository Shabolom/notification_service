package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	ServiceName    string `envconfig:"APP_NAME"`
	Debug          bool   `envconfig:"APP_DEBUG"`
	GRPCPort       string `envconfig:"APP_GRPC_ADDRESS"`
	Secret         string `envconfig:"APP_SECRET"`
	ResendAppKey   string `envconfig:"RESEND_API_KEY"`
	RabbitMQ       RabbitMQConfig
	Kafka          Kafka
	KafkaSerialize KafkaSerialize
}

type KafkaSerialize struct {
	Host string `envconfig:"SCHEMA_REGISTRY_HOST"`
	Port string `envconfig:"SCHEMA_REGISTRY_PORT"`
}
type Kafka struct {
	Brokers []string `envconfig:"KAFKA_BROKERS"`
	Topic   string   `envconfig:"KAFKA_TOPIC"`
	GroupID string   `envconfig:"KAFKA_GROUP_ID"`
}

type RabbitMQConfig struct {
	Host     string `envconfig:"RABBITMQ_HOST"`
	Port     string `envconfig:"RABBITMQ_PORT"`
	Username string `envconfig:"RABBITMQ_USER"`
	Password string `envconfig:"RABBITMQ_PASSWORD"`
	VHOST    string `envconfig:"RABBITMQ_VHOST"`
}

func FromEnv() (*Config, error) {
	cfg := new(Config)

	if err := envconfig.Process("", cfg); err != nil {
		return nil, fmt.Errorf("error while parse env config | %w", err)
	}

	return cfg, nil
}

func (c *Config) RabbitMQDSN() string {
	return fmt.Sprintf(
		"amqp://%s:%s@%s:%s/%s",
		c.RabbitMQ.Username,
		c.RabbitMQ.Password,
		c.RabbitMQ.Host,
		c.RabbitMQ.Port,
		c.RabbitMQ.VHOST,
	)
}

func (c *Config) SchemaRegisterDSN() string {
	return fmt.Sprintf("http://%v:%v",
		c.KafkaSerialize.Host,
		c.KafkaSerialize.Port)
}
