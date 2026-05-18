package utils

import amqp "github.com/rabbitmq/amqp091-go"

func WasRetried(msg amqp.Delivery, retryQueue string) bool {
	raw, ok := msg.Headers["x-death"]
	if !ok {
		return false
	}

	deaths, ok := raw.([]interface{})
	if !ok {
		return false
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

		if queue == retryQueue {
			return true
		}
	}

	return false
}
