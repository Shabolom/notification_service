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
	Postgres       PostgresConfig
	RabbitMQ       RabbitMQConfig
	Redis          RedisConfig
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
	GroupID string   `envconfig:"KAFKA_CONSUMER_GROUP"`
}

type RabbitMQConfig struct {
	Host     string `envconfig:"RABBITMQ_HOST"`
	Port     string `envconfig:"RABBITMQ_PORT"`
	Username string `envconfig:"RABBITMQ_USER"`
	Password string `envconfig:"RABBITMQ_PASSWORD"`
	VHOST    string `envconfig:"RABBITMQ_VHOST"`
}

type RedisConfig struct {
	RedisHost     string `envconfig:"REDIS_HOST"`
	RedisPort     string `envconfig:"REDIS_PORT"`
	RedisPassword string `envconfig:"REDIS_PASSWORD"`
}

type PostgresConfig struct {
	Host          string `envconfig:"POSTGRES_HOST"`
	Port          string `envconfig:"POSTGRES_PORT"`
	User          string `envconfig:"POSTGRES_USER"`
	Password      string `envconfig:"POSTGRES_PASSWORD"`
	Database      string `envconfig:"POSTGRES_DB"`
	SSLMode       string `envconfig:"POSTGRES_SSLMODE"`
	MaxConnection int    `envconfig:"POSTGRES_MAX_CONNECTION" default:"10"`
	MinConnection int    `envconfig:"POSTGRES_MIN_CONNECTION" default:"0"`
}

func FromEnv() (*Config, error) {
	cfg := new(Config)

	if err := envconfig.Process("", cfg); err != nil {
		return nil, fmt.Errorf("error while parse env config | %w", err)
	}

	return cfg, nil
}

func (c *Config) DatabaseURL() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.Postgres.User,
		c.Postgres.Password,
		c.Postgres.Host,
		c.Postgres.Port,
		c.Postgres.Database,
		c.Postgres.SSLMode,
	)
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

func (c *Config) RedisDSN() string {
	return fmt.Sprintf("%s:%s",
		c.Redis.RedisHost,
		c.Redis.RedisPort,
	)
}

func (c *Config) SchemaRegisterDSN() string {
	return fmt.Sprintf("http://%v:%v",
		c.KafkaSerialize.Host,
		c.KafkaSerialize.Port)
}
