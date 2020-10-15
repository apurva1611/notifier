package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, consumerTopic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    consumerTopic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func consume(reader *kafka.Reader) {
	fmt.Println("notifier start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("notifier read message at topic:%v partition:%v offset:%v \n	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

func main() {
	createDb()
	createTable()
	defer closeDB()

	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	consumerTopic := os.Getenv("consumerTopic")
	reader := getKafkaReader(kafkaURL, consumerTopic, "watch-group")
	go consume(reader)
	defer reader.Close()

	fmt.Println("start periodic notifications... !!")
	for {
		time.Sleep(60 * time.Second)

		notificationCount := notifier()
		fmt.Printf("total notifications sent := %d", notificationCount)
	}
}

func notifier() int {
	return 0
}
