package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/tarekbadrshalaan/GoKafka/kafka-go/db"
	"github.com/tarekbadrshalaan/GoKafka/kafka-go/model"

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
	if err != nil {
		log.Print(err.Error())
		continue
	}
	log.Print(inputNotifierAlertIDs)

	db.DeleteAlertsByZipcodeNotInInputSet(weatherTopicData.Zipcode, inputNotifierAlertIDs)
	if err != nil {
		log.Print(err.Error())
		continue
	}
}

// 2. now loop over all the alerts from the weatherTopicData
// 	a. if (the alert is new) then insert into alert table
// 	b. check weather data to see if the alert_triggered needs to change
// 		if updating the alert_triggered then also update the trigger_update_ts
// 		with current TS
// 		if alert_triggered is set to false then set alert_status to NOT_TRIGGERED
func updateAlertTableFromInputWeatherTopicData(weatherTopicData model.WeatherTopicData) {

	for i := range weatherTopicData.Watchs {
		for j := range weatherTopicData.Watchs[i].Alerts {
			notifierAlert := model.NotifierAlert{}
			notifierAlert.alertID = weatherTopicData.Watchs[i].Alerts[j].ID
			notifierAlert.zipcode = weatherTopicData.Zipcode
			notifierAlert.watchID = weatherTopicData.Watchs[i].ID
			notifierAlert.userID = weatherTopicData.Watchs[i].UserId
			notifierAlert.alertTriggered = false
			notifierAlert.fieldType = weatherTopicData.Watchs[i].Alerts[j].FieldType
			notifierAlert.operator = weatherTopicData.Watchs[i].Alerts[j].Operator
			notifierAlert.value = weatherTopicData.Watchs[i].Alerts[j].Value

			db.InsertAlert(notifierAlert)
			if err != nil {
				log.Print(err.Error())
				continue
			}
			db.updateAlertBasedOnWeatherData(notifierAlert, weatherTopicData.WeatherData.Main)
			if err != nil {
				log.Print(err.Error())
				continue
			}
		}
	}

}

func updateAlertBasedOnWeatherData(notifierAlert model.NotifierAlert, weather model.WeatherTopicData.WeatherData) {
	var data
	switch notifierAlert.fieldType {
		case "temp":
			data = weather.Main.Temp
		case "feels_like":
			data = main.feels_like
		case "temp_min":
			data = main.temp_min
		case "temp_max":
			data = main.temp_max
		case "pressure":
			data = main.pressure
		case "humidity":
			data = main.humidity

	}

	var result
	switch notifierAlert.operator {
		case "gt":
			result = data > notifierAlert.value 
		case "gte":
			result = data >= notifierAlert.value 
		case "eq":
			result = data == notifierAlert.value
		case "lt":
			result = data < notifierAlert.value 
		case "lte":
			result = data <= notifierAlert.value 

	}

	dbnotifieralert := db.getNotifierAlertByAlertID(notifierAlert.alertID)
	
	if (result == dbnotifieralert.alertTriggered) {
		t1 := time.Now()
		t2 := db.getTriggerUpdateTS(notifierAlert.alertID)
		t2, err := time.Parse("1000-01-01 00:00:00", t2)
		if err != nil {
			panic(err)
		}
		diff := t1.sub(t2)
		if(diff <= 60){

		}
	}

}


