package model

type UserStatus struct {
	userID      string
	alertStatus string
	//ENUM('silent', 'alerted', 'threshold') COLLATE utf8_unicode_ci NOT NULL,

	lastAlertSentTS string
}
