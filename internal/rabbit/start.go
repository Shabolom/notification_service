package rabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const MESSAGECOUNT = 10

func (r *Rabbit) GettingConsumers() (
	error error,
	loginConsumer <-chan amqp.Delivery,
	registerConsumer <-chan amqp.Delivery,
) {
	ch, err := r.CanalInit()
	if err != nil {
		r.logger.Error("Error initializing Rabbit", zap.Error(err))
		return err, nil, nil
	}

	err = ch.Qos(MESSAGECOUNT, 0, false)
	if err != nil {
		r.logger.Error("Error setting QoS", zap.Error(err))
		return err, nil, nil
	}

	registerMsgs, err := ch.Consume(
		"auth.register",
		"auth-consumer-register",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.logger.Error("Failed to register a consumer", zap.Error(err))
		return err, nil, nil
	}

	loginMsgs, err := ch.Consume(
		"auth.login.logs",
		"auth-consumer-login",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.logger.Warn("Failed to register a consumer", zap.Error(err))
		return err, nil, nil
	}

	return nil, loginMsgs, registerMsgs
}
