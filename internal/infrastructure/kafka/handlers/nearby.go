package handlers

import (
	"fmt"
	"log/slog"
	"love-signal-geo-data/pkg/logger/sl"

	topics "love-signal-geo-data/internal/infrastructure/kafka"
)

type Sender interface {
	Send(topic string, data any) error
}

// UsersNearby is a handler for detected nearby users.
type UsersNearby struct {
	log    *slog.Logger
	sender Sender
}

// NewUsersNearby returns new instance of UsersNearby handler.
func NewUsersNearby(
	log *slog.Logger,
	sender Sender,
) *UsersNearby {
	return &UsersNearby{
		log:    log,
		sender: sender,
	}
}

// SendToKafka sends detected nearby users data to kafka.
func (un *UsersNearby) SendToKafka(userIDs []int64) error {
	const op = "handlers.UsersNearby.SendToKafka"

	log := un.log.With(
		slog.String("op", op),
		slog.String("topic", topics.UsersNearbyTopic),
	)

	if err := un.sender.Send(topics.UsersNearbyTopic, userIDs); err != nil {
		log.Error("error sending to kafka users nearby", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
