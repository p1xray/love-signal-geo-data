package repository

import (
	"context"
	"fmt"
	"log/slog"
	"love-signal-geo-data/internal/dto"
	"love-signal-geo-data/internal/entity"
	"love-signal-geo-data/internal/infrastructure/converter"
	"love-signal-geo-data/internal/infrastructure/storage/models"
)

type CoordinatesStorage interface {
	CoordinatesByUserID(ctx context.Context, userID int64) ([]models.Coordinate, error)
	CreateCoordinate(ctx context.Context, coordinate models.Coordinate) (int64, error)
	RemoveCoordinates(ctx context.Context, ids []int64) error
	NearbyUsers(ctx context.Context, latitude, longitude float64, maxDistance int) ([]int64, error)
}

type Coordinates struct {
	log     *slog.Logger
	storage CoordinatesStorage
}

func NewCoordinatesRepository(log *slog.Logger, storage CoordinatesStorage) *Coordinates {
	return &Coordinates{
		log:     log,
		storage: storage,
	}
}

func (c *Coordinates) CoordinatesByUserID(ctx context.Context, userID int64) ([]dto.Coordinate, error) {
	const op = "repository.Coordinates.CoordinatesByUserID"

	coordinates, err := c.storage.CoordinatesByUserID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	coordinatesDTO := converter.ToCoordinatesDTO(coordinates)

	return coordinatesDTO, nil
}

func (c *Coordinates) SaveCoordinates(ctx context.Context, coordinates entity.CollectionCoordinates) error {
	const op = "repository.Coordinates.SaveCoordinate"

	coordinatesToRemove := coordinates.CoordinatesToRemove()
	if err := c.removeCoordinates(ctx, coordinatesToRemove); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	coordinatesToCreate := coordinates.CoordinatesToCreate()
	if err := c.createCoordinates(ctx, coordinatesToCreate); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (c *Coordinates) createCoordinates(ctx context.Context, coordinates []entity.Coordinate) error {
	const op = "repository.Coordinates.createCoordinates"

	coordinatesToCreate := converter.ToCoordinatesStorage(coordinates, models.CoordinateCreated())

	for _, coordinate := range coordinatesToCreate {
		if _, err := c.storage.CreateCoordinate(ctx, coordinate); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}

	return nil
}

func (c *Coordinates) removeCoordinates(ctx context.Context, coordinates []entity.Coordinate) error {
	const op = "repository.Coordinates.removeCoordinates"

	coordinateIDs := make([]int64, len(coordinates))
	for i, coordinate := range coordinates {
		coordinateIDs[i] = coordinate.ID
	}

	if err := c.storage.RemoveCoordinates(ctx, coordinateIDs); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (c *Coordinates) NearbyUsers(ctx context.Context, coordinate entity.Coordinate, maxDistance int) ([]int64, error) {
	const op = "repository.Coordinates.NearbyUsers"

	nearbyUsers, err := c.storage.NearbyUsers(ctx, coordinate.Latitude, coordinate.Longitude, maxDistance)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return nearbyUsers, nil
}
