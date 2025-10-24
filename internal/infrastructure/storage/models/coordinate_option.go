package models

import "time"

type CoordinateOption func(*Coordinate)

func CoordinateCreated() CoordinateOption {
	now := time.Now()
	return func(c *Coordinate) {
		c.CreatedAt = now
	}
}
