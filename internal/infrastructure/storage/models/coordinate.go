package models

import "time"

type Coordinate struct {
	ID        int64
	UserID    int64
	Latitude  float64
	Longitude float64
	CreatedAt time.Time
}
