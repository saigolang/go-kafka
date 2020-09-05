package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
)

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// defining a broker
	brokers := []string{"localhost:9092"}

	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "test"

	// passing a dummy json message
	dummy := Name{
		FirstName: "Foo",
		LastName:  "Bar",
	}

	b, err := json.Marshal(dummy)

	if err != nil {
		panic(err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Successfully stored the message in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

}

type Name struct {
	FirstName string
	LastName  string
}
