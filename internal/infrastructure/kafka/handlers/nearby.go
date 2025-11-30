package handlers

import (
	"fmt"
	"log/slog"
	"love-signal-geo-data/internal/interaction/kafka"
	"love-signal-geo-data/pkg/logger/sl"
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
		slog.String("topic", kafka.UsersNearbyTopic),
	)

	if err := un.sender.Send(kafka.UsersNearbyTopic, userIDs); err != nil {
		log.Error("error sending to kafka users nearby", sl.Err(err))

		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
