package rabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func (r *Rabbit) CanalInit() (*amqp.Channel, error) {
	ch, err := r.conn.Channel()
	if err != nil {
		_ = r.conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		"auth.events",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		"auth.register",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		"auth.login.logs",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	err = ch.QueueBind(
		"auth.register",
		"register",
		"auth.events",
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	err = ch.QueueBind(
		"auth.login.logs",
		"login",
		"auth.events",
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	return ch, nil
}
