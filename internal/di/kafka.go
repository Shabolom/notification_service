package di

import kafkaProducer "notification_service/internal/kafka-producer"

func (d *DI) GetKafkaProducer() *kafkaProducer.Kafka {
	if d.kafkaProducer != nil {
		return d.kafkaProducer
	}
	return kafkaProducer.NewKafka(d.NewProducer(), d.NewKafkaJSONSerializer(), d.Config().Kafka.Topic)
}
