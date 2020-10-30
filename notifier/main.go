package main

import (
	"fmt"
	"log"
	"net/http"
	"notifier/db"
	"notifier/kafka"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

// func getKafkaReader(kafkaURL, consumerTopic, groupID string) *kafka.Reader {
// 	brokers := strings.Split(kafkaURL, ",")
// 	return kafka.NewReader(kafka.ReaderConfig{
// 		Brokers:  brokers,
// 		GroupID:  groupID,
// 		Topic:    consumerTopic,
// 		MinBytes: 10e3, // 10KB
// 		MaxBytes: 10e6, // 10MB
// 	})
// }

// func consume(reader *kafka.Reader) {
// 	fmt.Println("notifier start consuming ... !!")
// 	for {
// 		m, err := reader.ReadMessage(context.Background())
// 		if err != nil {
// 			log.Fatalln(err)
// 		}
// 		fmt.Printf("notifier read message at topic:%v partition:%v offset:%v \n	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
// 	}
// }

func main() {
	db.Init()
	defer db.CloseDB()

	// get kafka writer using environment variables.
	kafkaURL := "kafka:9092"
	consumerTopic := "weather"
	consumerGroup := "weather-group"

	fmt.Println("starting to consume")
	go kafka.Consume(kafkaURL, consumerTopic, consumerGroup)
	//defer reader.Close()

	//fmt.Printf("consumer topic: " + consumerTopic)
	fmt.Println("start periodic notifications in every 1 min with user only alerted 1 time/hour... !!")
	go func() {
		for {
			time.Sleep(60 * time.Second)
			notifier()
		}
	}()

	router := SetupRouter()
	log.Fatal(router.Run(":8080"))
}

func SetupRouter() *gin.Engine {
	router := gin.Default()
	v1 := router.Group("/v1")
	v1.GET("/healthcheck", healthCheck)
	return router
}

func healthCheck(c *gin.Context) {
	kafkaURL := "kafka:9092"
	err := db.HealthCheck()
	if err != nil {
		c.JSON(http.StatusInternalServerError, "db health check failed.")
		os.Exit(5)
	}

	err = kafka.HealthCheck(kafkaURL)

	if err != nil {
		c.JSON(http.StatusInternalServerError, "kafka health check failed.")
		os.Exit(6)
	}

	c.JSON(http.StatusOK, "ok")
}

func notifier() {

	// cts = current time stamp
	cts := time.Now()

	// tuts = trigger_update_ts
	// last = last alert sent ts
	//
	// loop for every user is userStatus table {
	// 	select 'alerts' from alert table where alert.user_id = selected user from userStatus table
	// 		if 'alerts' above is empty
	// 			then the user can be deleted from the userStatus table
	// 				and
	// 				continue to next user (skip below logic)
	userStatusArr := db.GetAllUserStatus()
	for i := range userStatusArr {
		userAlerts := db.GetAlertsByUserId(userStatusArr[i].UserID)

		if len(userAlerts) == 0 {
			// if user has no alerts then delete user
			db.DeleteUser(userStatusArr[i].UserID)
			continue
		}

		usersActiveAlerts := db.GetAlertsByUserIdWhereAlertIsTriggered(userStatusArr[i].UserID)

		// 	if there are no 'alerts' from alert table for the given user where alert_triggered = true,
		// 		then set userStatus.alert_status = 'silent' for the given user
		if len(usersActiveAlerts) == 0 {
			db.UpdateUserAlertStatus(userStatusArr[i].UserID, "silent")
			continue
		}

		// 	if (cts - last <= 1 hour) {
		// 		then for every alerts where (alert_status is NOT_SENT && alert_triggered = true) {
		// 			set alert.alert_status = ALERT_IGNORED_TRESHOLD_REACHED
		// 		}
		// 	}
		if cts.Sub(userStatusArr[i].LastAlertSentTS).Minutes() <= 5 {
			for j := range usersActiveAlerts {
				if usersActiveAlerts[j].AlertStatus == "NOT_SENT" {
					db.UpdateAlertStatus(usersActiveAlerts[j].AlertID,
						"ALERT_IGNORED_TRESHOLD_REACHED")
				}
			}
		} else {
			//case when cts - last > 1 hour (meaning more than 1 hour has elapsed since user got notified)
			// 		so now
			// 		select 'alerts' where (alert_status is NOT_SENT && alert_triggered = true)
			// 			in ascending order of tuts
			for j := range usersActiveAlerts {
				if usersActiveAlerts[j].AlertStatus != "NOT_SENT" {
					continue
				}

				// 		then for each alert from 'alerts' {
				// 			1. if userStatus.alert_status = 'silent'
				// 				then set alert.alert_status = ALERT_SEND
				// 					set userStatus.alert_status = 'alerted'
				// 					set userStatus.last_alert_sent_ts = cts
				if userStatusArr[i].AlertStatus == "silent" {

					db.UpdateAlertStatus(usersActiveAlerts[j].AlertID, "ALERT_SEND")

					userStatusArr[i].AlertStatus = "alerted"
					userStatusArr[i].LastAlertSentTS = cts

					db.UpdateUserAlertStatus(userStatusArr[i].UserID, userStatusArr[i].AlertStatus)
					db.UpdateUserLastAlertSentTS(userStatusArr[i].UserID, cts)
				} else {
					// 	2. if userStatus.alert_status = 'alerted'
					// 	 then set alert.alert_status = ALERT_IGNORED_TRESHOLD_REACHED
					db.UpdateAlertStatus(usersActiveAlerts[j].AlertID,
						"ALERT_IGNORED_TRESHOLD_REACHED")
				}
			}
		}
	}
}
