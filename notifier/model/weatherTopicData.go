package model

type WeatherTopicData struct {
	Zipcode     string  `json:"zipcode" binding:"required"`
	WeatherData Weather `json:"weather_data" binding:"required"`
	Watchs      []WATCH `json:"watchs" binding:"required"`
}
