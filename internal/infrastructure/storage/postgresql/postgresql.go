package postgresql

import (
	"context"
	"fmt"
	"love-signal-geo-data/internal/config"
	"love-signal-geo-data/internal/infrastructure/storage/models"
	"love-signal-geo-data/pkg/postgresql"
)

// Storage provides access to PostgreSQL storage.
type Storage struct {
	pg *postgresql.Postgres
}

// New creates a new instance of the PostgreSQL store.
func New(cfg config.PostgreSQLConfig) (*Storage, error) {
	const op = "postgresql.New"

	pg, err := postgresql.New(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Storage{pg: pg}, nil
}

// Close closes connections to PostgreSQL.
func (s *Storage) Close() {
	s.pg.Close()
}

func (s *Storage) CoordinatesByUserID(ctx context.Context, userID int64) ([]models.Coordinate, error) {
	const op = "postgresql.CoordinatesByUserID"

	stmt := "select c.id, c.user_id, c.latitude, c.longitude, c.created_at from coordinates c where c.user_id = $1;"
	rows, err := s.pg.Pool.Query(ctx, stmt, userID)
	defer rows.Close()

	if err != nil {
		return []models.Coordinate{}, fmt.Errorf("%s: %w", op, err)
	}

	coordinates := make([]models.Coordinate, 0)
	for rows.Next() {
		coordinate := models.Coordinate{}
		err = rows.Scan(
			&coordinate.ID,
			&coordinate.UserID,
			&coordinate.Latitude,
			&coordinate.Longitude,
			&coordinate.CreatedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}

		coordinates = append(coordinates, coordinate)
	}

	return coordinates, nil
}

func (s *Storage) CreateCoordinate(ctx context.Context, coordinate models.Coordinate) (int64, error) {
	const op = "postgresql.CreateCoordinate"

	stmt := `
		insert into coordinates (user_id, latitude, longitude, created_at)
		values ($1, $2, $3, $4)
		returning id;
		`
	row := s.pg.Pool.QueryRow(ctx, stmt, coordinate.UserID, coordinate.Latitude, coordinate.Longitude, coordinate.CreatedAt)

	var newCoordinateID int64
	err := row.Scan(&newCoordinateID)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return newCoordinateID, nil
}

func (s *Storage) RemoveCoordinates(ctx context.Context, ids []int64) error {
	const op = "postgresql.RemoveCoordinate"

	stmt := "delete from coordinates where id = any($1);"
	_, err := s.pg.Pool.Exec(ctx, stmt, ids)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *Storage) NearbyUsers(ctx context.Context, latitude, longitude float64, maxDistance int) ([]int64, error) {
	const op = "postgresql.NearbyUsers"

	stmt := `
		with nearby (user_id, distance) as (
			select
				c.user_id,
				round(ST_DistanceSphere(ST_Point(latitude, longitude),
										ST_Point($1, $2))
				) as distance
			from coordinates c
		)
		select n.user_id
		from nearby n
		where n.distance <= $3 and n.distance <> 0;`

	rows, err := s.pg.Pool.Query(ctx, stmt, latitude, longitude, maxDistance)
	defer rows.Close()

	if err != nil {
		return []int64{}, fmt.Errorf("%s: %w", op, err)
	}

	userIDs := make([]int64, 0)
	for rows.Next() {
		var userID int64
		err = rows.Scan(&userID)

		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}

		userIDs = append(userIDs, userID)
	}

	return userIDs, nil
}
