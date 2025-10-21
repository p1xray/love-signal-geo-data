package repository

import (
	"context"
	"fmt"
	"log/slog"
	"love-signal-geo-data/internal/dto"
	"love-signal-geo-data/internal/entity"
)

type CoordinatesStorage interface {
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
	return nil, fmt.Errorf("not implemented")
}

func (c *Coordinates) SaveCoordinate(ctx context.Context, coordinate entity.Coordinate) error {
	return fmt.Errorf("not implemented")
}

func (c *Coordinates) NearbyUsers(ctx context.Context, userID int64) ([]int64, error) {
	return nil, fmt.Errorf("not implemented")
}
