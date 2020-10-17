package model

import "time"

type UserStatus struct {
	UserID      string
	AlertStatus string
	//ENUM('silent', 'alerted') COLLATE utf8_unicode_ci NOT NULL,

	LastAlertSentTS time.Time
}
