package coordinates

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"love-signal-geo-data/internal/dto"
	"love-signal-geo-data/internal/entity"
	"love-signal-geo-data/pkg/logger/sl"
)

type Repository interface {
	CoordinatesByUserID(ctx context.Context, userID int64) ([]dto.Coordinate, error)
	SaveCoordinates(ctx context.Context, coordinates entity.CollectionCoordinates) error
	NearbyUsers(ctx context.Context, coordinate entity.Coordinate, maxDistance int) ([]int64, error)
}

type Handler interface {
	// SendToKafka sends detected nearby users data to kafka.
	SendToKafka(userIDs []int64) error
}

// UseCase is a use-case for processing new user coordinates.
type UseCase struct {
	log         *slog.Logger
	repo        Repository
	handler     Handler
	maxDistance int
}

// New returns the new instance of use-case for processing new user coordinates.
func New(log *slog.Logger, repo Repository, handler Handler) *UseCase {
	return &UseCase{
		log:         log,
		repo:        repo,
		handler:     handler,
		maxDistance: 50, // TODO: get this from config
	}
}

// Execute processes the received new user coordinates.
func (uc *UseCase) Execute(ctx context.Context, dataAsBytes []byte) error {
	const op = "usecase.coordinates.Execute"

	log := uc.log.With(slog.String("op", op))

	var currentCoordinate dto.Coordinate
	if err := json.Unmarshal(dataAsBytes, &currentCoordinate); err != nil {
		log.Error("error unmarshalling user coordinates", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	previousCoordinates, err := uc.repo.CoordinatesByUserID(ctx, currentCoordinate.UserID)
	if err != nil {
		log.Error("error getting previous coordinates", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	collectionCoordinates := entity.NewCollectionCoordinates(currentCoordinate, previousCoordinates)

	collectionCoordinates.Filter()
	collectionCoordinates.Process()

	if err = uc.repo.SaveCoordinates(ctx, collectionCoordinates); err != nil {
		log.Error("error saving coordinates", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	nearbyUsers, err := uc.repo.NearbyUsers(ctx, collectionCoordinates.CurrentCoordinate, uc.maxDistance)
	if err != nil {
		log.Error("error getting nearby users", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	if err = uc.handler.SendToKafka(nearbyUsers); err != nil {
		log.Error("error sending nearby users to kafka", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
