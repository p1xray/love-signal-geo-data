package entity

import "love-signal-geo-data/internal/dto"

type CollectionCoordinates struct {
	CurrentCoordinate   Coordinate
	PreviousCoordinates []Coordinate
}

func NewCollectionCoordinates(current dto.Coordinate, previous []dto.Coordinate) CollectionCoordinates {
	previousEntities := make([]Coordinate, len(previous))
	for i, previousCoordinate := range previous {
		previousEntities[i] = NewCoordinate(previousCoordinate)
	}

	return CollectionCoordinates{
		CurrentCoordinate:   NewCoordinate(current),
		PreviousCoordinates: previousEntities,
	}
}

func (cc *CollectionCoordinates) Filter() {
	// TODO: implement this.
}
