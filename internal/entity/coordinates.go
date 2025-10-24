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

func (cc *CollectionCoordinates) Process() {
	cc.CurrentCoordinate.SetToCreate()

	for i := range cc.PreviousCoordinates {
		cc.PreviousCoordinates[i].SetToRemove()
	}
}

func (cc *CollectionCoordinates) CoordinatesToCreate() []Coordinate {
	allCoordinates := cc.allCoordinates()

	coordinatesToCreate := make([]Coordinate, 0)
	for _, coordinate := range allCoordinates {
		if coordinate.IsToCreate() {
			coordinatesToCreate = append(coordinatesToCreate, coordinate)
		}
	}

	return coordinatesToCreate
}

func (cc *CollectionCoordinates) CoordinatesToRemove() []Coordinate {
	allCoordinates := cc.allCoordinates()

	coordinatesToRemove := make([]Coordinate, 0)
	for _, coordinate := range allCoordinates {
		if coordinate.IsToRemove() {
			coordinatesToRemove = append(coordinatesToRemove, coordinate)
		}
	}

	return coordinatesToRemove
}

func (cc *CollectionCoordinates) allCoordinates() []Coordinate {
	coordinates := make([]Coordinate, 0, len(cc.PreviousCoordinates)+1)
	for _, previousCoordinate := range cc.PreviousCoordinates {
		coordinates = append(coordinates, previousCoordinate)
	}

	coordinates = append(coordinates, cc.CurrentCoordinate)

	return coordinates
}
