package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "topic_test"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	//conn.SetWriteDeadline(time.Now().Add(time.Second * 10))

	for {
		time.Sleep(time.Second * 1)
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte("hello from one")},
		)
		if err != nil {
			log.Fatal("failed to write messages : ", err)
			break
		}
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer: ", err)
	}
}
