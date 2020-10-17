package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"notifier/model"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
		alert_status ENUM('NOT_SENT', 'ALERT_SEND', 'ALERT_IGNORED_DUPLICATE', 'ALERT_IGNORED_TRESHOLD_REACHED') COLLATE utf8_unicode_ci NOT NULL default 'NOT_SENT',
		field_type ENUM('temp', 'feels_like', 'temp_min', 'temp_max', 'pressure','humidity') COLLATE utf8_unicode_ci NOT NULL,
		operator ENUM('gt', 'gte', 'eq', 'lt', 'lte') COLLATE utf8_unicode_ci NOT NULL,
		value float NOT NULL,
		PRIMARY KEY (alert_id)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;`)

	// create table userStatus
	_, err2 := db.Exec(`CREATE TABLE IF NOT EXISTS notifierdb.userStatus(
		user_id varchar(100) NOT NULL,
		alert_status ENUM('silent', 'alerted') COLLATE utf8_unicode_ci NOT NULL default 'silent',
		last_alert_sent_ts datetime NOT NULL default '1000-01-01 00:00:00',
		PRIMARY KEY (user_id)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;`)

	if err1 != nil {
		panic(err1)
	}
	if err2 != nil {
		panic(err2)
	}

}

func InsertUser(userId string) {
	insert, err := db.Prepare(`INSERT INTO notifierdb.userStatus(user_id) VALUES (?)`)
	if err != nil {
		log.Printf(err.Error())
	}

	_, err = insert.Exec(userId)
	if err != nil {
		log.Printf(err.Error())
	}
}

func DeleteUser(userId string) {
	delete, err := db.Prepare("DELETE FROM notifierdb.userStatus WHERE user_id=?")
	delete.Exec(userId)
	if err != nil {
		log.Printf(err.Error())
	}
}

func UpdateUserAlertStatus(userId string, alertStatus string) {
	update, err := db.Prepare(`UPDATE notifierdb.userStatus SET alert_status=? WHERE user_id=? VALUES (?, ?)`)

	update.Exec(alertStatus, userId)
	if err != nil {
		log.Printf(err.Error())
	}
}

func UpdateUserLastAlertSentTS(userId string, lastAlertSentTS time.Time) {
	update, err := db.Prepare(`UPDATE notifierdb.userStatus SET last_alert_sent_ts=? WHERE user_id=? VALUES (?, ?)`)

	update.Exec(lastAlertSentTS, userId)
	if err != nil {
		log.Printf(err.Error())
	}
}

func GetAllUserStatus() []model.UserStatus {
	userStatusArr := make([]model.UserStatus, 0)

	results, err := db.Query(`SELECT user_id, alert_status, last_alert_sent_ts
							FROM notifierdb.userStatus`)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}

	for results.Next() {
		userStatus := model.UserStatus{}
		err = results.Scan(&userStatus.UserID, &userStatus.AlertStatus,
			&userStatus.LastAlertSentTS)
		if err != nil {
			continue
		}
		userStatusArr = append(userStatusArr, userStatus)
	}

	return userStatusArr
}

func GetAlertsByUserId(userId string) []model.NotifierAlert {
	notifierAlerts := make([]model.NotifierAlert, 0)

	results, err := db.Query(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, tigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE user_id = ?`, userId)
	if err != nil {
		log.Printf(err.Error())
	}

	for results.Next() {
		notifierAlert := model.NotifierAlert{}
		err = results.Scan(
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
			break
		}

		notifierAlerts = append(notifierAlerts, notifierAlert)
	}

	return notifierAlerts
}

func GetAlertsByUserIdWhereAlertIsTriggered(userId string) []model.NotifierAlert {
	notifierAlerts := make([]model.NotifierAlert, 0)

	results, err := db.Query(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, tigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE user_id = ? AND alert_status = true 
	ORDER BY trigger_update_ts ASC`, userId)
	if err != nil {
		log.Printf(err.Error())
	}

	for results.Next() {
		notifierAlert := model.NotifierAlert{}
		err = results.Scan(
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
			break
		}

		notifierAlerts = append(notifierAlerts, notifierAlert)
	}

	return notifierAlerts
}

func DeleteAlertsByZipcodeNotInInputSet(zipcode string, notifierAlertsIDsToExclude string) bool {
	delete, err := db.Prepare("DELETE FROM notifierdb.alert WHERE zipcode=? AND alert_id NOT IN (?)")

	delete.Exec(zipcode, notifierAlertsIDsToExclude)
	if err != nil {
		log.Printf(err.Error())
		return false
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
		notifierAlert.WatchID,
		notifierAlert.UserID,
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

func GetNotifierAlertByAlertID(alertID string) model.NotifierAlert {
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
	}

	return notifierAlert
}

func UpdateNotifierAlertByAlertID(alertID string) model.NotifierAlert {
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
	}
	return notifierAlert
}

func GetTriggerUpdateTS(alertID string) time.Time {
	var triggerUpdateTS time.Time

	err := db.QueryRow(`SELECT trigger_update_ts FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(&triggerUpdateTS)
	if err != nil {
		log.Printf(err.Error())
	}
	return triggerUpdateTS
}

func UpdateAlertTriggerUpdateTS(alertID string, updatedTriggeredTS time.Time) {

	update, err1 := db.Prepare(`UPDATE notifierdb.alert SET trigger_update_ts=? WHERE alert_id=? VALUES (?, ?)`)

	if err1 != nil {
		log.Printf(err1.Error())
	}

	_, err2 := update.Exec(updatedTriggeredTS.String(), alertID)

	if err2 != nil {
		log.Printf(err2.Error())
	}
}

func UpdateAlertStatus(alertID string, alertStatus string) {

	update, err1 := db.Prepare(`UPDATE notifierdb.alert SET alert_status=? WHERE alert_id=? VALUES (?, ?)`)

	if err1 != nil {
		log.Printf(err1.Error())
	}

	_, err2 := update.Exec(alertStatus, alertID)

	if err2 != nil {
		log.Printf(err2.Error())
	}
}

func UpdateAlertTriggered(alertID string, alertTriggered bool) {

	if alertTriggered {
		update, err1 := db.Prepare(`UPDATE notifierdb.alert SET alert_triggered=true WHERE alert_id=? VALUES (?)`)
		if err1 != nil {
			log.Printf(err1.Error())
		}
		_, err2 := update.Exec(alertID)

		if err2 != nil {
			log.Printf(err2.Error())
		}
	} else {
		update, err1 := db.Prepare(`UPDATE notifierdb.alert SET alert_triggered=false WHERE alert_id=? VALUES (?)`)
		if err1 != nil {
			log.Printf(err1.Error())
		}
		_, err2 := update.Exec(alertID)

		if err2 != nil {
			log.Printf(err2.Error())
		}
	}

}

// func getAlertTrigger(alertID string) string {

// 	notifierAlertTrigger, err := db.QueryRow(`SELECT alert_triggerd FROM notifierdb.alert WHERE alert_id = ?`, alertId).Scan(&notifierAlert.AlertTriggered)
// 	if err != nil {
// 		log.Printf(err.Error())
// 		return nil
// 	}
// 	return notifierAlertTrigger
// }

// func updateUserAlertStatus(userID string, userAlertStatus bool) bool {

// 	_, err := db.Prepare(`UPDATE notifierdb.userStatus SET alert_status=? WHERE user_id=? VALUES (?, ?)`)

// 	if err != nil {
// 		log.Printf(err.Error())
// 		return false
// 	}

// 	update, err := db.Exec(userAlertStatus, userID)

// 	return true
// }
