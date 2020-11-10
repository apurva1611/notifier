package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"notifier/db"
	"notifier/model"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func HealthCheck(kafkaURL string) error {
	conn, err := kafka.Dial("tcp", kafkaURL)
	if err != nil {
		return err
	}

	conn.Close()
	return nil
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func Consume(kafkaURL, topic, groupID string) {
	fmt.Println("getting kafka reader")
	reader := getKafkaReader(kafkaURL, topic, groupID)
	fmt.Println("got kafka reader")
	defer reader.Close()
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			continue
		}

		log.Printf("CONSUME Topic: %s, Message ID %s", topic, string(m.Key))

		weatherTopicData := model.WeatherTopicData{}
		err = json.Unmarshal(m.Value, &weatherTopicData)
		if err != nil {
			log.Print(err.Error())
			continue
		}

		deleteOrphanAlerts(weatherTopicData)
		updateAlertTableFromInputWeatherTopicData(weatherTopicData)
	}
}

// 1. for the given zipcode find alerts from the alert table
// 	that are not there in the watch array from the weatherTopicData
// 	-> delete these alerts from the alert table
// 	as they are deleted from the upstream service as well.
// 	Note - This will also remove watchs from the alert table
// 	which were deleted on upstream for the given zipcode
func deleteOrphanAlerts(weatherTopicData model.WeatherTopicData) {
	// notifierAlerts := db.GetAlertsByZipcode(weatherTopicData.zipcode)

	inputNotifierAlertIDs := ""

	for i := range weatherTopicData.Watchs {
		for j := range weatherTopicData.Watchs[i].Alerts {
			inputNotifierAlertIDs += "'" + weatherTopicData.Watchs[i].Alerts[j].ID + "',"
		}
	}
	inputNotifierAlertIDs = strings.TrimRight(inputNotifierAlertIDs, ",")

	log.Print(inputNotifierAlertIDs)
	fmt.Println("deleting orphan alerts")
	db.DeleteAlertsByZipcodeNotInInputSet(weatherTopicData.Zipcode, inputNotifierAlertIDs)

	// s := strings.Split(inputNotifierAlertIDs, ",")
	// fmt.Println(s)

	// for k := range s {
	// 	notifierAlert := model.NotifierAlert{}
	// 	fmt.Println(s[k])
	// 	notifierAlert = *db.GetNotifierAlertByAlertID(s[k])

	// 	fmt.Println("inserting notifierAlert")
	// 	db.InsertAlert(notifierAlert)
	// }

}

// 2. now loop over all the alerts from the weatherTopicData
// 	a. if (the alert is new) then insert into alert table
// 	b. check weather data to see if the alert_triggered needs to change
// 		if updating the alert_triggered then also update the trigger_update_ts
// 		with current TS
// 		if alert_triggered is set to false then set alert_status to NOT_TRIGGERED
func updateAlertTableFromInputWeatherTopicData(weatherTopicData model.WeatherTopicData) {
	arriveTS := time.Now()
	defaultTS := arriveTS.AddDate(-3, 0, 0)

	fmt.Println("arriveTS")
	fmt.Println(arriveTS)

	for i := range weatherTopicData.Watchs {
		out, _ := json.Marshal(weatherTopicData.Watchs[i])
		fmt.Println(string(out))
		fmt.Printf("userID in notifier" + weatherTopicData.Watchs[i].UserId)
		log.Print(weatherTopicData.Watchs[i].UserId)
		fmt.Println("inserting userid")
		db.InsertUser(weatherTopicData.Watchs[i].UserId)

		for j := range weatherTopicData.Watchs[i].Alerts {
			notifierAlert := model.NotifierAlert{}
			notifierAlert.AlertID = weatherTopicData.Watchs[i].Alerts[j].ID
			notifierAlert.Zipcode = weatherTopicData.Zipcode
			notifierAlert.WatchID = weatherTopicData.Watchs[i].ID
			notifierAlert.UserID = weatherTopicData.Watchs[i].UserId
			notifierAlert.AlertTriggered = true
			notifierAlert.TriggerUpdateTS = defaultTS
			notifierAlert.AlertStatus = "NOT_SENT"
			notifierAlert.FieldType = weatherTopicData.Watchs[i].Alerts[j].FieldType
			notifierAlert.Operator = weatherTopicData.Watchs[i].Alerts[j].Operator
			notifierAlert.Value = weatherTopicData.Watchs[i].Alerts[j].Value
			fmt.Println("inserting alert")
			db.InsertAlert(notifierAlert)
			fmt.Println("updating alert based on weatherdata")
			updateAlertBasedOnWeatherData(notifierAlert, weatherTopicData.WeatherData, arriveTS)
		}
	}

}

func updateAlertBasedOnWeatherData(notifierAlert model.NotifierAlert, weather model.Weather, arriveTS time.Time) {
	var data float32
	switch notifierAlert.FieldType {
	case "temp":
		data = weather.Main.Temp
	case "feels_like":
		data = weather.Main.FeelsLike
	case "temp_min":
		data = weather.Main.TempMin
	case "temp_max":
		data = weather.Main.TempMax
	case "pressure":
		data = weather.Main.Pressure
	case "humidity":
		data = weather.Main.Humidity

	}

	var result bool
	switch notifierAlert.Operator {
	case "gt":
		result = data > notifierAlert.Value
	case "gte":
		result = data >= notifierAlert.Value
	case "eq":
		result = data == notifierAlert.Value
	case "lt":
		result = data < notifierAlert.Value
	case "lte":
		result = data <= notifierAlert.Value
	}

	dbnotifieralert := db.GetNotifierAlertByAlertID(notifierAlert.AlertID)
	fmt.Println("updating alert based on userdata")
	fmt.Println(dbnotifieralert.AlertTriggered)
	if result == true && dbnotifieralert.AlertTriggered == true {
		fmt.Println("if weatherdata/result and alert triggered true")
		triggerUpdateTS := db.GetTriggerUpdateTS(notifierAlert.AlertID)
		diff := arriveTS.Sub(triggerUpdateTS)

		// if the alert was triggered less than an hour back compared to weather data sent
		// then this alert can be marked as duplicate
		if diff.Minutes() < 5 {
			// set alert_status to ALERT_IGNORED_DUPLICATE
			fmt.Println("but diff less than one hour then alert_ignored_dup")
			db.UpdateAlertStatus(notifierAlert.AlertID, "ALERT_IGNORED_DUPLICATE")
		} else {
			fmt.Println("but if not less than one hour then update alert trigger ts and alert status")
			// update alert trigger_update_ts in db with this -> arriveTS
			db.UpdateAlertTriggerUpdateTS(notifierAlert.AlertID, arriveTS)
			// set alert_status to NOT_SENT
			db.UpdateAlertStatus(notifierAlert.AlertID, "NOT_SENT")
		}
	} else if result == false && dbnotifieralert.AlertTriggered == true {
		fmt.Println("if weatherdata/result false and alert triggered true")
		fmt.Println("then update alert trigger to false and alert status to notsent")
		// set alert_triggered to false
		db.UpdateAlertTriggered(notifierAlert.AlertID, false)
		// set alert_status to NOT_SENT
		db.UpdateAlertStatus(notifierAlert.AlertID, "NOT_SENT")
	} else if result == true && dbnotifieralert.AlertTriggered == false {
		fmt.Println("if weatherdata/result true and alert triggered false")
		fmt.Println("then update alert trigger to true and alert status to notsent and trigger ts to arrivets")
		// set alert_triggered to true
		db.UpdateAlertTriggered(notifierAlert.AlertID, true)
		// update alert trigger_update_ts with arriveTS
		db.UpdateAlertTriggerUpdateTS(notifierAlert.AlertID, arriveTS)
		// set alert_status to NOT_SENT
		db.UpdateAlertStatus(notifierAlert.AlertID, "NOT_SENT")
	} else {
		// when both false do nothing
		fmt.Println("do nothing if alert data and weatherdata false")
	}

}
