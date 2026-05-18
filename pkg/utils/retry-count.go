package utils

import amqp "github.com/rabbitmq/amqp091-go"

func RetryCount(msg amqp.Delivery, queueName string) int64 {
	raw, ok := msg.Headers["x-death"]
	if !ok {
		return 0
	}

	deaths, ok := raw.([]interface{})
	if !ok {
		return 0
	}

	for _, death := range deaths {
		deathMap, ok := death.(amqp.Table)
		if !ok {
			continue
		}

		queue, ok := deathMap["queue"].(string)
		if !ok {
			continue
		}

		if queue != queueName {
			continue
		}

		count, ok := deathMap["count"].(int64)
		if !ok {
			return 0
		}

		return count
	}

	return 0
}
