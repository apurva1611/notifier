package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/tarekbadrshalaan/GoKafka/kafka-go/db"

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
	db.Init()
	defer db.CloseDB()

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

	// cts = current time stamp
	// tuts = trigger_update_ts
	// last = last alert sent ts
	//
	// loop for every user is userStatus table {
	// 	select 'alerts' from alert table where alert.user_id = selected user from userStatus table
	// 		if 'alerts' above is empty
	// 			then the user can be deleted from the userStatus table
	// 				and
	// 				continue to next user (skip below logic)

	// 	if there are no 'alerts' from alert table for the given user where alert_triggered = true,
	// 		then set userStatus.alert_status = 'silent' for the given user

	// 	if (cts - last <= 1 min) {
	// 		then for every alerts where (alert_status is NOT_TRIGGERED && alert_triggered = true) {
	// 			set alert.alert_status = ALERT_IGNORED_TRESHOLD_REACHED
	// 		}
	// 	} else {
	// 		case when cts - last > 1 min (meaning more than 1 min has elapsed since user got notified)
	// 		so now
	// 		select 'alerts' where (alert_status is NOT_TRIGGERED && alert_triggered = true)
	// 			in ascending order of tuts
	// 		then for each alert from 'alerts' {
	// 			1. if userStatus.alert_status = 'silent'
	// 				then set alert.alert_status = ALERT_SEND
	// 					set userStatus.alert_status = 'alerted'
	// 					set userStatus.last_alert_sent_ts = cts

	// 			2. if userStatus.alert_status = 'alerted'
	// 				then set alert.alert_status = ALERT_IGNORED_TRESHOLD_REACHED
	// 					set userStatus.alert_status = 'threshold'

	// 			3. if userStatus.alert_status = 'threshold'
	// 				then set alert.alert_status = ALERT_IGNORED_TRESHOLD_REACHED

	// 		}
	// 	}

	return 0
}
