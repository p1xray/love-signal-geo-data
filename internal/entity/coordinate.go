package entity

import (
	"love-signal-geo-data/internal/dto"
	"love-signal-geo-data/internal/enum"
)

type Coordinate struct {
	ID        int64
	UserID    int64
	Latitude  float64
	Longitude float64

	dataStatus enum.DataStatus
}

func NewCoordinate(data dto.Coordinate) Coordinate {
	return Coordinate{
		ID:        data.ID,
		UserID:    data.UserID,
		Latitude:  data.Latitude,
		Longitude: data.Longitude,
	}
}

func (c *Coordinate) SetToCreate() {
	c.dataStatus = enum.ToCreate
}

func (c *Coordinate) SetToRemove() {
	c.dataStatus = enum.ToRemove
}

func (c *Coordinate) IsToCreate() bool {
	return c.dataStatus == enum.ToCreate
}

func (c *Coordinate) IsToRemove() bool {
	return c.dataStatus == enum.ToRemove
}
