package db

import (
	"context"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"notifier/model"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

const (
	username = "root"
	password = "pass1234"
	port     = ":3306"
	dbname   = "notifierdb"
)

func Init() {
	createDb()
	createTable()
}

func dsn() string {
	rdsurl := os.Getenv("rdsurl")
	hostname := rdsurl + port
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", username, password, hostname, dbname)
}

func openDB() {
	var err error
	db, err = sql.Open("mysql", dsn())
	if err != nil {
		log.Error("Error %s when opening DB\n", err)
		panic(err)
	}
}

func CloseDB() {
	db.Close()
}

func HealthCheck() error {
	err := db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func createDb() {
	openDB()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)

	if err != nil {
		log.Error("Error %s when creating DB\n", err)
		return
	}

	no, err := res.RowsAffected()
	if err != nil {
		log.Error("Error %s when fetching rows", err)
		return
	}
	log.Error("rows affected %d\n", no)

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelFunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	err = db.PingContext(ctx)
	if err != nil {
		log.Error("Errors %s pinging DB", err)
		return
	}
	log.Error("Connected to DB %s successfully\n", dbname)
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

func queryByID(userId string) bool {

	fmt.Println("inside querybyid")

	err := db.QueryRow(`SELECT user_id FROM notifierdb.userStatus WHERE user_id = ?`, userId).Scan(&userId)
	if err != nil {
		log.Error("Get user_id in notififer.userStatus failed")
		log.Error(err.Error())
		return false
	}
	log.Info("Get user_id in notififer.userStatus")
	return true
}

func InsertUser(userId string) {

	fmt.Printf("inside InsertUser - querybyuserId value: ")
	fmt.Println(queryByID(userId))

	if queryByID(userId) == false {

		insert, err := db.Prepare(`INSERT INTO notifierdb.userStatus(user_id) VALUES (?)`)

		if err != nil {
			log.Error("Insert user_id in notififer.userStatus failed")
			log.Error(err.Error())
		}

		fmt.Println("executing insert user")
		_, err = insert.Exec(userId)
		if err != nil {
			log.Error("Insert user_id in notififer.userStatus failed")
			log.Error(err.Error())
		}
		log.Info("Insert user_id in notififer.userStatus")
	}
}

func DeleteUser(userId string) {
	delete, err := db.Prepare("DELETE FROM notifierdb.userStatus WHERE user_id=?")
	delete.Exec(userId)
	if err != nil {
		log.Error("Delete query from notififer.userStatus")
		log.Error(err.Error())
	}
		log.Info("Delete query from notififer.userStatus")
}

func UpdateUserAlertStatus(userId string, alertStatus string) {
	update, err := db.Prepare(`UPDATE notifierdb.userStatus SET alert_status=? WHERE user_id=?`)

	update.Exec(alertStatus, userId)
	if err != nil {
		log.Error("Update query from notififer.userStatus failed")
		log.Error(err.Error())
	}

		log.Info("Update query from notififer.userStatus")
}

func UpdateUserLastAlertSentTS(userId string, lastAlertSentTS time.Time) {
	update, err := db.Prepare(`UPDATE notifierdb.userStatus SET last_alert_sent_ts=? WHERE user_id=?`)

	update.Exec(lastAlertSentTS, userId)
	if err != nil {
		log.Error("Update query from notififer.userStatus failed")
		log.Error(err.Error())
	}

		log.Info("Update query from notififer.userStatus")
}

func GetAllUserStatus() []model.UserStatus {
	userStatusArr := make([]model.UserStatus, 0)

	results, err := db.Query(`SELECT user_id, alert_status, last_alert_sent_ts
							FROM notifierdb.userStatus`)
	if err != nil {
		log.Error("Get all user query from notififer.userStatus failed")
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
	log.Info("Get all user query from notififer.userStatus")
	return userStatusArr
}

func GetAlertsByUserId(userId string) []model.NotifierAlert {
	notifierAlerts := make([]model.NotifierAlert, 0)

	results, err := db.Query(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, trigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE user_id = ?`, userId)
	if err != nil {
		log.Error("Get alert from notififer.alert based on userId failed")
		log.Error(err.Error())
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
			log.Error("Get alert from notififer.alert based on userId failed")
			log.Error(err.Error())
			break
		}

		notifierAlerts = append(notifierAlerts, notifierAlert)
	}
	log.Info("Get alert from notififer.alert based on userId")
	return notifierAlerts
}

func GetAlertsByUserIdWhereAlertIsTriggered(userId string) []model.NotifierAlert {
	notifierAlerts := make([]model.NotifierAlert, 0)

	results, err := db.Query(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, trigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE user_id = ? AND alert_status = true 
	ORDER BY trigger_update_ts ASC`, userId)
	if err != nil {
		log.Error("Get alerts from notififer.alert based on userId and alert_status=true failed")
		log.Error(err.Error())
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
			log.Error("Get alerts from notififer.alert based on userId and alert_status=true failed")
			log.Error(err.Error())
			break
		}

		notifierAlerts = append(notifierAlerts, notifierAlert)
	}
    log.Info("Get alerts from notififer.alert based on userId and alert_status=true")
	return notifierAlerts
}

func DeleteAlertsByZipcodeNotInInputSet(zipcode string, notifierAlertsIDsToExclude string) bool {
	delete, err := db.Prepare("DELETE FROM notifierdb.alert WHERE zipcode=? AND alert_id NOT IN (?)")

	delete.Exec(zipcode, notifierAlertsIDsToExclude)
	if err != nil {
		log.Error("Delete alerts from notififer.alert based on zipcode and alert_id failed")
		log.Error(err.Error())
		return false
	}
	log.Info("Get alerts from notififer.alert based on userId and alert_id")
	return true
}

func queryByAlertID(AlertID string) bool {

	fmt.Println("inside querybyAlertID")

	err := db.QueryRow(`SELECT alert_id FROM notifierdb.alert WHERE alert_id = ?`, AlertID).Scan(&AlertID)
	if err != nil {
		log.Error("Get alert_id from notififer.alert")
		log.Error(err.Error())
		return false
	}
	log.Info("Get alert_id from notififer.alert")
	return true
}

func InsertAlert(notifierAlert model.NotifierAlert) bool {

	if queryByAlertID(notifierAlert.AlertID) == false {

		insert, err := db.Prepare(`INSERT INTO notifierdb.alert(alert_id, zipcode, watch_id, user_id, alert_triggered, trigger_update_ts, alert_status, field_type, operator, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		fmt.Println("alert status value: ")
		fmt.Println(notifierAlert.AlertStatus)
		fmt.Println("trigger update value: ")
		fmt.Println(notifierAlert.TriggerUpdateTS)

		if err != nil {
			log.Error("Insert alert in notififer.alert failed")
			log.Error(err.Error())
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
			log.Error("Insert alert in notififer.alert failed")
			log.Error(err.Error())
			return false
		}
		log.Info("Insert alert in notififer.alert")
		return true
	}
	return false
}

func GetNotifierAlertByAlertID(alertID string) *model.NotifierAlert {
	notifierAlert := model.NotifierAlert{}

	err := db.QueryRow(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, trigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(
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
		log.Error("Get alert from notififer.alert based on alert_id")
		log.Error(err.Error())
	}
    log.Info("Get alert from notififer.alert based on alert_id")
	return &notifierAlert
}

func updateNotifierAlertByAlertID(alertID string) *model.NotifierAlert {
	notifierAlert := model.NotifierAlert{}

	err := db.QueryRow(`SELECT alert_id, zipcode, watch_id, user_id, alert_triggered, trigger_update_ts, alert_status, field_type, operator, value FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(
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
		log.Error("Get alert from notififer.alert based on alert_id")
		log.Error(err.Error())
	}
	log.Info("Get alert from notififer.alert based on alert_id")
	return &notifierAlert
}

func GetTriggerUpdateTS(alertID string) time.Time {
	var triggerUpdateTS time.Time

	err := db.QueryRow(`SELECT trigger_update_ts FROM notifierdb.alert WHERE alert_id = ?`, alertID).Scan(&triggerUpdateTS)
	if err != nil {
		log.Error("Get trigger_update_ts from notififer.alert based on alert_id")
		log.Error(err.Error())
	}
	log.Info("Get trigger_update_ts from notififer.alert based on alert_id query")
	return triggerUpdateTS
}

func UpdateAlertTriggerUpdateTS(alertID string, updatedTriggeredTS time.Time) {
	fmt.Println("updating alert trigger ts")
	update, err1 := db.Prepare(`UPDATE notifierdb.alert SET trigger_update_ts=? WHERE alert_id=?`)
	if err1 != nil {
		log.Error(" Update notififer.alert based on alert_id query failed")
		log.Error(err1.Error())
	}

	_, err2 := update.Exec(updatedTriggeredTS, alertID)
	if err2 != nil {
		log.Error(" Update notififer.alert based on alert_id query failed")
		log.Error(err2.Error())
	}
		log.Info(" Update notififer.alert based on alert_id query")

}

func UpdateAlertStatus(alertID string, alertStatus string) {
	fmt.Println("updating alert status to not sent")
	update, err1 := db.Prepare(`UPDATE notifierdb.alert SET alert_status=? WHERE alert_id=?`)

	if err1 != nil {
		log.Error("updating notifier.alert query failed")
		log.Error(err1.Error())
	}

	_, err2 := update.Exec(alertStatus, alertID)

	if err2 != nil {
		log.Error("updating notifier.alert query failed")
		log.Error(err2.Error())
	}
	log.Info(" Update notififer.alert table based on alert_id  and alert_status query")
}

func UpdateAlertTriggered(alertID string, alertTriggered bool) {
	fmt.Println("update alert trigger")
	if alertTriggered {
		fmt.Println("update alert trigger true")
		update, err1 := db.Prepare(`UPDATE notifierdb.alert SET alert_triggered=true WHERE alert_id=?`)
		if err1 != nil {
			log.Error(" Update notififer.alert table based on alert_id  and alert_triggered query failed")
			log.Printf(err1.Error())
		}
		_, err2 := update.Exec(alertID)

		if err2 != nil {
			log.Error(" Update notififer.alert table based on alert_id  and alert_triggered query failed")
			log.Printf(err2.Error())
		}
	} else {
		fmt.Println("update alert trigger false")
		update, err1 := db.Prepare(`UPDATE notifierdb.alert SET alert_triggered=false WHERE alert_id=?`)
		if err1 != nil {
			log.Error(" Update notififer.alert table based on alert_id  and alert_triggered query failed")
			log.Error(err1.Error())
		}
		_, err2 := update.Exec(alertID)

		if err2 != nil {
			log.Error(" Update notififer.alert table based on alert_id  and alert_triggered query failed")
			log.Error(err2.Error())
		}
	}
	log.Info(" Update notififer.alert table based on alert_id  and alert_triggered query")
}
