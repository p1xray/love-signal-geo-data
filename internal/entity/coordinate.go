package entity

import "love-signal-geo-data/internal/dto"

type Coordinate struct {
	UserID    int64
	Latitude  float64
	Longitude float64
}

func NewCoordinate(data dto.Coordinate) Coordinate {
	return Coordinate{
		UserID:    data.UserID,
		Latitude:  data.Latitude,
		Longitude: data.Longitude,
	}
}
