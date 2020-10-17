package model

type Weather struct {
	Main struct {
		Temp      float32 `json:"temp"`
		FeelsLike float32 `json:"feels_like"`
		TempMin   float32 `json:"temp_min"`
		TempMax   float32 `json:"temp_max"`
		Pressure  float32 `json:"pressure"`
		Humidity  float32 `json:"humidity"`
	} `json:"main"`
}
