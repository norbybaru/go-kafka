package kafka

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

type Reader struct {
	Consumer sarama.Consumer
}

func NewKafkaReader(brokerUrl []string) * Reader {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokerUrl, config)

	if err != nil {
		log.Fatal(err)
	}

	return &Reader{
		Consumer: conn,
	}
}

func (k *Reader) ConsumeMessages(topic string) {
	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	partition, err := k.Consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer connected!")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgReadCount := 0
	// Get signal for finish
	exitCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-partition.Errors():
				fmt.Println(err)
			case msg := <-partition.Messages():
				msgReadCount++
				fmt.Printf(
					"Received message Count %d: | Topic(%s) | Message(%s) \n",
					msgReadCount,
					string(msg.Topic),
					string(msg.Value),
				)

			case <-sigchan:
				fmt.Println("Interrupt is detected")
				exitCh <- struct{}{}
			}
		}
	}()

	<-exitCh
	fmt.Println("Processed", msgReadCount, "messages")

	if err := partition.Close(); err != nil {
		log.Fatal(err)
	}
}