package rabbit

import amqp "github.com/rabbitmq/amqp091-go"

func (r *Rabbit) PublishToDLQ(msg *amqp.Delivery, routingKey string) error {
	return r.ch.Publish(
		"auth.events.final.dlx",
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  msg.ContentType,
			Body:         msg.Body,
			Headers:      msg.Headers,
			DeliveryMode: amqp.Persistent,
			Timestamp:    msg.Timestamp,
			MessageId:    msg.MessageId,
		},
	)
}
