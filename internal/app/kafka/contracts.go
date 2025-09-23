package kafka

import "context"

type (
	// UserCoordinatesHandler is a handler for getting new user coordinates.
	UserCoordinatesHandler interface {
		// Execute processes the received new user coordinates.
		Execute(ctx context.Context, dataAsBytes []byte) error
	}
)
