package model

type NotifierAlert struct {
	alertID         string
	zipcode         string
	watchID         string
	userID          string
	alertTriggered  bool
	triggerUpdateTS string
	alertStatus     string
	//ENUM('NOT_TRIGGERED', 'ALERT_SEND', 'ALERT_IGNORED_DUPLICATE', 'ALERT_IGNORED_TRESHOLD_REACHED') COLLATE utf8_unicode_ci NOT NULL default 'NOT_TRIGGERED',

	fieldType string
	//ENUM('temp', 'feels_like', 'temp_min', 'temp_max', 'pressure','humidity') COLLATE utf8_unicode_ci NOT NULL,

	operator string
	//ENUM('gt', 'gte', 'eq', 'lt', 'lte') COLLATE utf8_unicode_ci NOT NULL,

	value int
}
