package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Comment struct {
    Text string `form:"text" json:"text"`
}

func handleComment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	payload, err := io.ReadAll(r.Body)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		msg := fmt.Sprintf("could not read message body: %s", err)
		fmt.Println(msg)
		if _, err := w.Write([]byte(fmt.Sprintf(`{"message": "%s"}`, msg))); err != nil {
			fmt.Println("error sending response:", err)
		}
		return
	}
	
	var comment Comment
	if err := json.Unmarshal(payload, &comment); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("could not unmarshal message request:: %s", err)
		fmt.Println(msg)

		if _, err := w.Write([]byte(fmt.Sprintf(`{"message": "%s"}`, msg))); err != nil {
			fmt.Println("error sending response:", err)
		}
		return
	}

	//fmt.Println("Payload: ", comment)

	// convert body into bytes and send it to kafka
    cmtInBytes, err := json.Marshal(comment)
	//fmt.Println("comment byes:", cmtInBytes)
	pushMessageToQueue("comment", cmtInBytes)

	w.WriteHeader(http.StatusCreated)
	msg := fmt.Sprintf("Comment successfully stored")
	if _, err := w.Write([]byte(fmt.Sprintf(`{"message": "%s"}`, msg))); err != nil {
		fmt.Println("error sending response:", err)
	}
}

var producer sarama.SyncProducer

func main() {
	brokerUrl := []string{"localhost:9093"}
	producer = connect(brokerUrl)

	defer producer.Close()

	http.HandleFunc("/api/v1/comments", handleComment)

	fmt.Println("starting web server on localhost:3000")
	if err := http.ListenAndServe(":3000", nil); err != nil {
		log.Fatalf("error starting web server: %s", err)
	}

}

// type Kafka struct {
// 	Connection sarama.SyncProducer
// }

func connect(brokerUrl []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokerUrl, config)

	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func pushMessageToQueue(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

