package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Host      string
	Topic     string
	Partition int
}

func NewProducer(host, topic string, partition int) *Producer {
	return &Producer{
		Host:      host,
		Topic:     topic,
		Partition: partition,
	}
}

func (p *Producer) Connect(c context.Context) (*kafka.Conn, error) {
	return kafka.DialLeader(c, "tcp", p.Host, p.Topic, p.Partition)
}

var (
	host      = "localhost:9092"
	topic     = "topic_test"
	partition = 0
)

func main() {
	producer := NewProducer(host, topic, partition)
	conn, err := producer.Connect(context.Background())

	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

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
