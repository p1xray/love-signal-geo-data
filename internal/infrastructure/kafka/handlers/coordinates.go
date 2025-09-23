package handlers

import (
	"context"
	"log/slog"
)

// UserCoordinatesTopic is the name of the topic in kafka where data for the user coordinates is stored.
const UserCoordinatesTopic = "user-geo-data"

// UserCoordinates is a handler for getting new user coordinates.
type UserCoordinates struct {
	log *slog.Logger
}

// NewUserCoordinates returns new instance of UserCoordinates handler.
func NewUserCoordinates(log *slog.Logger) *UserCoordinates {
	return &UserCoordinates{
		log: log,
	}
}

// Execute processes the received new user coordinates.
func (uc *UserCoordinates) Execute(ctx context.Context, dataAsBytes []byte) error {
	// TODO: implement this.

	return nil
}
