package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type SingleBroker struct {
	Host      string
	Topic     string
	Partition int
	MaxBytes  int
}

func NewSingleBroker(host, topic string, partition, maxbytes int) *SingleBroker {
	return &SingleBroker{
		Host:      host,
		Topic:     topic,
		Partition: partition,
		MaxBytes:  maxbytes,
	}
}

func (s *SingleBroker) config() kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:   []string{s.Host},
		Topic:     s.Topic,
		Partition: s.Partition,
		MaxBytes:  s.MaxBytes,
	}
}

func (s *SingleBroker) Reader() *kafka.Reader {
	return kafka.NewReader(
		s.config(),
	)
}

var (
	topic     = "topic_test"
	host      = "localhost:9092"
	maxbytes  = 10e6
	partition = 0
)

func main() {
	broker := NewSingleBroker(host, topic, partition, int(maxbytes))

	r := broker.Reader()

	r.SetOffset(42)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader: ", err)
	}
}
