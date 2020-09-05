package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
)

func main() {

	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true

	// defining the brokers
	brokers := []string{"localhost:9092"}

	// creating the consumer

	consumer, err := sarama.NewConsumer(brokers, config)

	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "test"

	consumerPartition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt)

	messageCount := 0

	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumerPartition.Errors():
				fmt.Println(err)

			case msg := <-consumerPartition.Messages():
				messageCount++
				fmt.Println("messages recieved are", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interruption has happened")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan

	fmt.Println("Total number of messages processed are ", messageCount)

}
