package kafka

import "context"

type (
	// UserCoordinatesUseCase is a use-case for processing new user coordinates.
	UserCoordinatesUseCase interface {
		// Execute processes the received new user coordinates.
		Execute(ctx context.Context, dataAsBytes []byte) error
	}
)
