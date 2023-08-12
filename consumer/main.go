package main

import (
	"github.com/IBM/sarama"
	"github.com/norbybaru/go-kafka/pkg/kafka"
)

var consumer sarama.Consumer

func main() {
	brokerUrl := []string{"localhost:9093"}

	reader := kafka.NewKafkaReader(brokerUrl)
	reader.ConsumeMessages("comment")
}