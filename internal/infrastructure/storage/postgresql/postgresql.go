package postgresql

import (
	"fmt"
	"love-signal-geo-data/internal/config"
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
