package dto

type Coordinate struct {
	ID        int64
	UserID    int64   `json:"user_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
