package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"notifier/model"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

var db *sql.DB

const (
	username = "root"
	password = "pass1234"
	hostname = "localhost:3306"
	dbname   = "notifierdb"
)

func Init() {
	createDb()
	createTable()
}

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbname)
}

func openDB() {
	var err error
	db, err = sql.Open("mysql", dsn())
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		panic(err)
	}
}

func CloseDB() {
	db.Close()
}

func createDb() {
	openDB()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)

	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return
	}

	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return
	}
	log.Printf("rows affected %d\n", no)

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelFunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return
	}
	log.Printf("Connected to DB %s successfully\n", dbname)
}

func createTable() {

	// create table alert
	_, err1 := db.Exec(`CREATE TABLE IF NOT EXISTS notifierdb.alert(
		alert_id varchar(100) NOT NULL,
		zipcode varchar(100) COLLATE utf8_unicode_ci NOT NULL,
		watch_id varchar(100) COLLATE utf8_unicode_ci NOT NULL,
		user_id varchar(100) COLLATE utf8_unicode_ci NOT NULL,
		alert_triggered boolean default false,
		trigger_update_ts datetime NOT NULL default '1000-01-01 00:00:00',
		alert_status ENUM('NOT_TRIGGERED', 'ALERT_SEND', 'ALERT_IGNORED_DUPLICATE', 'ALERT_IGNORED_TRESHOLD_REACHED') COLLATE utf8_unicode_ci NOT NULL default 'NOT_TRIGGERED',
		field_type ENUM('temp', 'feels_like', 'temp_min', 'temp_max', 'pressure','humidity') COLLATE utf8_unicode_ci NOT NULL,
		operator ENUM('gt', 'gte', 'eq', 'lt', 'lte') COLLATE utf8_unicode_ci NOT NULL,
		value int NOT NULL,
		PRIMARY KEY (alert_id)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;`)

	// create table userStatus
	_, err2 := db.Exec(`CREATE TABLE IF NOT EXISTS notifierdb.userStatus(
		user_id varchar(100) NOT NULL,
		alert_status ENUM('silent', 'alerted', 'threshold') COLLATE utf8_unicode_ci NOT NULL,
		last_alert_sent_ts datetime NOT NULL,
		PRIMARY KEY (user_id),
		)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;`)

	if err1 != nil {
		panic(err1)
	}
	if err2 != nil {
		panic(err2)
	}

}

// func GetAlertsByZipcode(zipcode string) []model.NotifierAlert {
// 	notifierAlerts := make([]model.NotifierAlert, 0)

// 	results, err := db.Query(`SELECT watch_id, user_id, zipcode, alert_id
// 							FROM notifierdb.alert WHERE zipcode = ?`, zipcode)
// 	if err != nil {
// 		log.Printf(err.Error())
// 		return nil
// 	}

// 	for results.Next() {
// 		notifierAlert := model.NotifierAlert{}
// 		err = results.Scan(&notifierAlert.WatchID, &notifierAlert.UserID, &notifierAlert.zipcode, &notifierAlert.alertID)
// 		if err != nil {
// 			continue
// 		}
// 		notifierAlerts = append(notifierAlerts, notifierAlert)
// 	}

// 	return notifierAlerts
// }

func DeleteAlertsByZipcodeNotInInputSet(zipcode string, notifierAlertsIDsToExclude string) bool {
	delete, err := db.Prepare("DELETE FROM notifierdb.alert WHERE zipcode=? AND alert_id NOT IN (?)")

	delete.Exec(zipcode, notifierAlertsIDsToExclude)
	if err != nil {
		log.Printf(err.Error())
		return false
	}
	return true
}

func InsertWatch(watch model.WATCH) bool {
	insert, err := db.Prepare(`INSERT INTO notifierdb.watch(watch_id, user_id, zipcode, alerts, watch_created, watch_updated) VALUES (?, ?, ?, ?, ?, ?)`)

	log.Print("insert p")

	if err != nil {
		log.Print(err.Error())
		return false
	}

	log.Print("insert prepare")
	alertsJson, _ := json.Marshal(&watch.Alerts)

	res, err := insert.Exec(watch.ID, watch.UserId, watch.Zipcode, alertsJson, watch.WatchCreated, watch.WatchUpdated)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	log.Print(res.RowsAffected())

	for i := range watch.Alerts {
		uid, _ := uuid.NewRandom()
		watch.Alerts[i].ID = uid.String()
		watch.Alerts[i].WatchId = watch.ID
		watch.Alerts[i].AlertCreated = watch.WatchCreated
		watch.Alerts[i].AlertUpdated = watch.WatchCreated
	}

	for _, a := range watch.Alerts {
		// have to convert Alert `a` to NotifierAlert
		if !InsertAlert(a) {
			return false
		}
	}

	return true
}

func InsertAlert(notifierAlert model.NotifierAlert) bool {
	insert, err := db.Prepare(`INSERT INTO notifierdb.alert(alert_id, zipcode, watch_id, user_id, alert_triggered, tigger_update_ts, alert_status, field_type, operator, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)

	if err != nil {
		log.Printf(err.Error())
		return false
	}
	_, err = insert.Exec(
		notifierAlert.AlertID,
		notifierAlert.Zipcode,
		notifierAlert.UserID,
		notifierAlert.WatchID,
		notifierAlert.Operator,
		notifierAlert.AlertTriggered,
		notifierAlert.TriggerUpdateTS,
		notifierAlert.AlertStatus,
		notifierAlert.FieldType,
		notifierAlert.Operator,
		notifierAlert.Value)

	if err != nil {
		log.Printf(err.Error())
		return false
	}

	return true
}

func GetNotifierAlertByAlertID(alertID string) *model.NotifierAlert {
	notifierAlert := model.NotifierAlert{}

	err := db.QueryRow(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, tigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(
		&notifierAlert.AlertID,
		&notifierAlert.Zipcode,
		&notifierAlert.WatchID,
		&notifierAlert.UserID,
		&notifierAlert.AlertTriggered,
		&notifierAlert.TriggerUpdateTS,
		&notifierAlert.AlertStatus,
		&notifierAlert.FieldType,
		&notifierAlert.Operator,
		&notifierAlert.Value)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}

	return &notifierAlert
}

func updateNotifierAlertByAlertID(alertID string) *model.NotifierAlert {
	notifierAlert := model.NotifierAlert{}

	err := db.QueryRow(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, tigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(
		&notifierAlert.AlertID,
		&notifierAlert.Zipcode,
		&notifierAlert.WatchID,
		&notifierAlert.UserID,
		&notifierAlert.AlertTriggered,
		&notifierAlert.TriggerUpdateTS,
		&notifierAlert.AlertStatus,
		&notifierAlert.FieldType,
		&notifierAlert.Operator,
		&notifierAlert.Value)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}

	return &notifierAlert
}

func GetTriggerUpdateTS(alertID string) string {
	notifierAlert := model.NotifierAlert{}

	err := db.QueryRow(`SELECT trigger_update_ts FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(&notifierAlert.TriggerUpdateTS)

	if err != nil {
		return ""
	}
	return notifierAlert.TriggerUpdateTS
}

func updateTriggerUpdateTS(alertID string, updatedTriggeredTS string) bool {

	_, err := db.Prepare(`UPDATE notifierdb.alert SET trigger_update_ts=? WHERE alert_id=? VALUES (?, ?)`)

	if err != nil {
		log.Printf(err.Error())
		return false
	}

	_, err = db.Exec(updatedTriggeredTS, alertID)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	return true
}

func getAlertTrigger(alertID string) bool {
	notifierAlert := model.NotifierAlert{}

	err := db.QueryRow(`SELECT alert_triggerd FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(&notifierAlert.AlertTriggered)
	if err != nil {
		log.Printf(err.Error())
		return false
	}
	return notifierAlert.AlertTriggered
}

func updateUserAlertStatus(userID string, userAlertStatus string) bool {

	_, err := db.Prepare(`UPDATE notifierdb.userStatus SET alert_status=? WHERE user_id=? VALUES (?, ?)`)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	_, err = db.Exec(userAlertStatus, userID)
	if err != nil {
		log.Printf(err.Error())
		return false
	}

	return true
}
