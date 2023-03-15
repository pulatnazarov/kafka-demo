package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"time"
)

type UserRegisteredEvent struct {
	UserID  string `json:"user_id"`
	Email   string `json:"email"`
	encoded []byte
}

func (e UserRegisteredEvent) Encode() ([]byte, error) {
	var err error
	if e.encoded == nil {
		e.encoded, err = json.Marshal(e)
		if err != nil {
			return nil, err
		}
	}

	return e.encoded, nil
}

func (e UserRegisteredEvent) Length() int {
	e.Encode()
	return len(e.encoded)
}

func main() {
	fmt.Println("i am the producer")

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

	for {
		if _, _, err = producer.SendMessage(generateMessage()); err != nil {
			fmt.Println("failed to send message:", err)
		}
	}
}

func generateMessage() *sarama.ProducerMessage {
	var name, greeting string
	fmt.Print("Enter your name: ")
	name, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("Enter your greeting: ")
	greeting, err = bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	name = name[:len(name)-1]
	greeting = greeting[:len(greeting)-1]

	return &sarama.ProducerMessage{
		Topic:     "greetings",
		Key:       sarama.StringEncoder(name),
		Value:     sarama.StringEncoder(greeting),
		Offset:    1,
		Partition: 0,
		Timestamp: time.Now(),
	}
}
