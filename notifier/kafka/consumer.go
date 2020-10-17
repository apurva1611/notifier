package kafka

import (
	"context"
	"encoding/json"
	"log"
	"notifier/db"
	"notifier/model"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

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
	reader := getKafkaReader(kafkaURL, topic, groupID)
	defer reader.Close()
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			continue
		}

		//log.Print(string(m.Value))

		weatherTopicData := model.WeatherTopicData{}
		err = json.Unmarshal(m.Value, &weatherTopicData)
		if err != nil {
			log.Print(err.Error())
			continue
		}

		log.Print(weatherTopicData)

		deleteOrphanAlerts(weatherTopicData)

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

	db.DeleteAlertsByZipcodeNotInInputSet(weatherTopicData.Zipcode, inputNotifierAlertIDs)
<<<<<<< HEAD

=======
>>>>>>> a6d3867ed984bc8746b0e48b06e0c39cd618183b
}

// 2. now loop over all the alerts from the weatherTopicData
// 	a. if (the alert is new) then insert into alert table
// 	b. check weather data to see if the alert_triggered needs to change
// 		if updating the alert_triggered then also update the trigger_update_ts
// 		with current TS
// 		if alert_triggered is set to false then set alert_status to NOT_TRIGGERED
func updateAlertTableFromInputWeatherTopicData(weatherTopicData model.WeatherTopicData) {
	arriveTS := time.Now()

	for i := range weatherTopicData.Watchs {
		db.InsertUser(weatherTopicData.Watchs[i].UserId)

		for j := range weatherTopicData.Watchs[i].Alerts {
			notifierAlert := model.NotifierAlert{}
			notifierAlert.AlertID = weatherTopicData.Watchs[i].Alerts[j].ID
			notifierAlert.Zipcode = weatherTopicData.Zipcode
			notifierAlert.WatchID = weatherTopicData.Watchs[i].ID
			notifierAlert.UserID = weatherTopicData.Watchs[i].UserId
			notifierAlert.AlertTriggered = false
			notifierAlert.FieldType = weatherTopicData.Watchs[i].Alerts[j].FieldType
			notifierAlert.Operator = weatherTopicData.Watchs[i].Alerts[j].Operator
			notifierAlert.Value = weatherTopicData.Watchs[i].Alerts[j].Value

			db.InsertAlert(notifierAlert)

<<<<<<< HEAD
			updateAlertBasedOnWeatherData(notifierAlert, weatherTopicData.WeatherData, arriveTS)
=======
			updateAlertBasedOnWeatherData(notifierAlert, weatherTopicData)
>>>>>>> a6d3867ed984bc8746b0e48b06e0c39cd618183b
		}
	}

}

<<<<<<< HEAD
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
=======
func updateAlertBasedOnWeatherData(notifierAlert model.NotifierAlert, weather model.WeatherTopicData) {
	var data float32
	switch notifierAlert.FieldType {
	case "temp":
		data = weather.WeatherData.Main.Temp
	case "feels_like":
		data = weather.WeatherData.Main.FeelsLike
	case "temp_min":
		data = weather.WeatherData.Main.TempMin
	case "temp_max":
		data = weather.WeatherData.Main.TempMax
	case "pressure":
		data = float32(weather.WeatherData.Main.Pressure)
	case "humidity":
		data = float32(weather.WeatherData.Main.Humidity)
	}

	var result bool
	val := float32(notifierAlert.Value)
	switch notifierAlert.Operator {
	case "gt":
		result = data > val
	case "gte":
		result = data >= val
	case "eq":
		result = data == val
	case "lt":
		result = data < val
	case "lte":
		result = data <= val

>>>>>>> a6d3867ed984bc8746b0e48b06e0c39cd618183b
	}

	dbnotifieralert := db.GetNotifierAlertByAlertID(notifierAlert.AlertID)

<<<<<<< HEAD
	if result == true && true == dbnotifieralert.AlertTriggered {
=======
	if result == dbnotifieralert.AlertTriggered {
		t1 := time.Now()
		t2 := db.GetTriggerUpdateTS(notifierAlert.AlertID)
		t3, err := time.Parse("1000-01-01 00:00:00", t2)
		if err != nil {
			panic(err)
		}
		diff := t1.Sub(t3)
		if diff <= 60 {
>>>>>>> a6d3867ed984bc8746b0e48b06e0c39cd618183b

		triggerUpdateTS := db.GetTriggerUpdateTS(notifierAlert.AlertID)
		diff := arriveTS.Sub(triggerUpdateTS)

		// if the alert was triggered less than an hour back compared to weather data sent
		// then this alert can be marked as duplicate
		if diff.Hours() < 1 {
			// set alert_status to ALERT_IGNORED_DUPLICATE
			db.UpdateAlertStatus(notifierAlert.AlertID, "ALERT_IGNORED_DUPLICATE")
		} else {
			// update alert trigger_update_ts in db with this -> arriveTS
			db.UpdateAlertTriggerUpdateTS(notifierAlert.AlertID, arriveTS)
			// set alert_status to NOT_SENT
			db.UpdateAlertStatus(notifierAlert.AlertID, "NOT_SENT")
		}
	} else if result == false && true == dbnotifieralert.AlertTriggered {
		// set alert_triggered to false
		db.UpdateAlertTriggered(notifierAlert.AlertID, false)
		// set alert_status to NOT_SENT
		db.UpdateAlertStatus(notifierAlert.AlertID, "NOT_SENT")
	} else if result == true && false == dbnotifieralert.AlertTriggered {
		// set alert_triggered to true
		db.UpdateAlertTriggered(notifierAlert.AlertID, true)
		// update alert trigger_update_ts with arriveTS
		db.UpdateAlertTriggerUpdateTS(notifierAlert.AlertID, arriveTS)
		// set alert_status to NOT_SENT
		db.UpdateAlertStatus(notifierAlert.AlertID, "NOT_SENT")
	} else {
		// when both false do nothing
	}

}
