package converter

import (
	"love-signal-geo-data/internal/dto"
	"love-signal-geo-data/internal/entity"
	"love-signal-geo-data/internal/infrastructure/storage/models"
)

func ToCoordinatesDTO(coordinates []models.Coordinate) []dto.Coordinate {
	coordinatesDTO := make([]dto.Coordinate, len(coordinates))
	for i, coordinate := range coordinates {
		coordinatesDTO[i] = ToCoordinateDTO(coordinate)
	}

	return coordinatesDTO
}

func ToCoordinateDTO(coordinate models.Coordinate) dto.Coordinate {
	return dto.Coordinate{
		ID:        coordinate.ID,
		UserID:    coordinate.UserID,
		Latitude:  coordinate.Latitude,
		Longitude: coordinate.Longitude,
	}
}

func ToCoordinatesStorage(coordinates []entity.Coordinate, setters ...models.CoordinateOption) []models.Coordinate {
	coordinatesStorage := make([]models.Coordinate, len(coordinates))
	for i, coordinate := range coordinates {
		coordinatesStorage[i] = ToCoordinateStorage(coordinate, setters...)
	}

	return coordinatesStorage
}

func ToCoordinateStorage(coordinate entity.Coordinate, setters ...models.CoordinateOption) models.Coordinate {
	coordinateStorage := models.Coordinate{
		ID:        coordinate.ID,
		UserID:    coordinate.UserID,
		Latitude:  coordinate.Latitude,
		Longitude: coordinate.Longitude,
	}

	for _, setter := range setters {
		setter(&coordinateStorage)
	}

	return coordinateStorage
}
