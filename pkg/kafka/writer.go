package kafka

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Writer struct {
	Producer sarama.SyncProducer
}

func NewKafkaWriter(brokerUrl []string) *Writer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokerUrl, config)

	if err != nil {
		log.Fatal(err)
	}

	return &Writer{
		Producer: conn,
	}
}

func (k *Writer) PushMessageToQueue(topic string, message []byte) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := k.Producer.SendMessage(msg)

	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}