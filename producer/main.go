package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/norbybaru/go-kafka/pkg/kafka"
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

	// convert body into bytes and send it to kafka
    cmtInBytes, err := json.Marshal(comment)

	if writer == nil {
		log.Fatal("Kafka Writer not instantiated")
	}

	writer.PushMessageToQueue("comment", cmtInBytes)

	w.WriteHeader(http.StatusCreated)
	msg := fmt.Sprintf("Comment successfully stored")
	if _, err := w.Write([]byte(fmt.Sprintf(`{"message": "%s"}`, msg))); err != nil {
		fmt.Println("error sending response:", err)
	}
}

var writer *kafka.Writer

func main() {
	brokerUrl := []string{"localhost:9093"}
	writer = kafka.NewKafkaWriter(brokerUrl)
	defer writer.Producer.Close()

	http.HandleFunc("/api/v1/comments", handleComment)

	fmt.Println("starting web server on localhost:3000")
	if err := http.ListenAndServe(":3000", nil); err != nil {
		log.Fatalf("error starting web server: %s", err)
	}

}
