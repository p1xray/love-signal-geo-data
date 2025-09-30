package coordinates

import (
	"context"
	"encoding/json"
	"log/slog"
	"love-signal-geo-data/internal/dto"
	"love-signal-geo-data/internal/entity"
	"love-signal-geo-data/pkg/logger/sl"
)

type Repository interface {
	CoordinatesByUserID(ctx context.Context, userID int64) ([]dto.Coordinate, error)
	SaveCoordinate(ctx context.Context, coordinate entity.Coordinate) error
	NearbyUsers(ctx context.Context, userID int64) ([]int64, error)
}

type Handler interface {
	// SendToKafka sends detected nearby users data to kafka.
	SendToKafka(userIDs []int64) error
}

// UseCase is a use-case for processing new user coordinates.
type UseCase struct {
	log     *slog.Logger
	repo    Repository
	handler Handler
}

// New returns the new instance of use-case for processing new user coordinates.
func New(log *slog.Logger, repo Repository, handler Handler) *UseCase {
	return &UseCase{
		log:     log,
		repo:    repo,
		handler: handler,
	}
}

// Execute processes the received new user coordinates.
func (uc *UseCase) Execute(ctx context.Context, dataAsBytes []byte) error {
	const op = "usecase.coordinates.Execute"

	log := uc.log.With(slog.String("op", op))

	var coordinateDTO dto.Coordinate
	if err := json.Unmarshal(dataAsBytes, &coordinateDTO); err != nil {
		log.Error("error unmarshalling user coordinates", sl.Err(err))
	}

	previousCoordinates, err := uc.repo.CoordinatesByUserID(ctx, coordinateDTO.UserID)
	if err != nil {
		log.Error("error getting previous coordinates", sl.Err(err))

		return err
	}

	collectionCoordinates := entity.NewCollectionCoordinates(coordinateDTO, previousCoordinates)

	collectionCoordinates.Filter()

	if err = uc.repo.SaveCoordinate(ctx, collectionCoordinates.CurrentCoordinate); err != nil {
		log.Error("error saving coordinates", sl.Err(err))

		return err
	}

	nearbyUsers, err := uc.repo.NearbyUsers(ctx, collectionCoordinates.CurrentCoordinate.UserID)
	if err != nil {
		log.Error("error getting nearby users", sl.Err(err))

		return err
	}

	if err = uc.handler.SendToKafka(nearbyUsers); err != nil {
		log.Error("error sending nearby users to kafka", sl.Err(err))

		return err
	}

	return nil
}
