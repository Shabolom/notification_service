package utils

import amqp "github.com/rabbitmq/amqp091-go"

func ReachedRetryLimit(msg *amqp.Delivery, retryQueue string, retryCount int64) bool {
	raw, ok := msg.Headers["x-death"]
	// пример что содержит в себе ключ
	//x-death:
	//[
	//  {
	//    count: 3,
	//    exchange: "auth.retry.exchange",
	//    queue: "auth.register.retry",
	//    reason: "expired",
	//    routing-keys: ["register.retry"],
	//    time: ...
	//  }
	//]
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

		reason, ok := deathMap["reason"].(string)
		if !ok {
			continue
		}

		count, ok := deathMap["count"].(int64)
		if !ok {
			continue
		}

		if queue == retryQueue && reason == "expired" && count >= retryCount {
			return true
		}
	}

	return false
}
