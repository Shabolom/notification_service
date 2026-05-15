package kafkaProducer

func (k *Kafka) Close() {
	k.producer.Close()
}
