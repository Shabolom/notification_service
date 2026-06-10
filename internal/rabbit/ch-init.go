package rabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	LoginQueueName         = "auth.login.logs"
	RegisterQueueName      = "auth.register"
	LoginQueueRetryName    = "auth.login.logs.retry"
	RegisterQueueRetryName = "auth.register.retry"
	LoginQueueDLQName      = "auth.login.logs.dlq"
	RegisterQueueDLQName   = "auth.register.dlq"
)

func (r *Rabbit) CanalInit() (*amqp.Channel, error) {
	if r.ch != nil {
		return r.ch, nil
	}

	ch, err := r.conn.Channel()
	if err != nil {
		_ = r.conn.Close()
		return nil, err
	}

	//----------------ExchangeDeclare------------------------

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

	err = ch.ExchangeDeclare(
		"auth.events.dlx",
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

	err = ch.ExchangeDeclare(
		"auth.events.final.dlx",
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

	//----------------QueueDeclare------------------------

	_, err = ch.QueueDeclare(
		RegisterQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "auth.events.dlx",
			"x-dead-letter-routing-key": "register.retry",
		},
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		LoginQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "auth.events.dlx",
			"x-dead-letter-routing-key": "login.logs.retry",
		},
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		RegisterQueueRetryName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":             int32(1000),
			"x-dead-letter-exchange":    "auth.events",
			"x-dead-letter-routing-key": "register",
		},
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		LoginQueueRetryName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":             int32(1000),
			"x-dead-letter-exchange":    "auth.events",
			"x-dead-letter-routing-key": "login",
		},
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	_, err = ch.QueueDeclare(
		RegisterQueueDLQName,
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
		LoginQueueDLQName,
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

	//----------------BIND------------------------

	err = ch.QueueBind(
		RegisterQueueName,
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
		LoginQueueName,
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

	err = ch.QueueBind(
		RegisterQueueRetryName,
		"register.retry",
		"auth.events.dlx",
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	err = ch.QueueBind(
		LoginQueueRetryName,
		"login.logs.retry",
		"auth.events.dlx",
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	err = ch.QueueBind(
		RegisterQueueDLQName,
		"register.dlq",
		"auth.events.final.dlx",
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	err = ch.QueueBind(
		LoginQueueDLQName,
		"login.logs.dlq",
		"auth.events.final.dlx",
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = r.conn.Close()
		return nil, err
	}

	r.ch = ch

	return r.ch, nil
}
