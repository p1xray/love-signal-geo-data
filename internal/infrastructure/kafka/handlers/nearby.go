package handlers

import (
	"log/slog"
	"love-signal-geo-data/pkg/kafka"
)

// UsersNearby is a handler for detected nearby users.
type UsersNearby struct {
	log                *slog.Logger
	receiveDataChannel chan<- kafka.Message
}

// NewUsersNearby returns new instance of UsersNearby handler.
func NewUsersNearby(log *slog.Logger, receiveDataChannel chan<- kafka.Message) *UsersNearby {
	return &UsersNearby{
		log:                log,
		receiveDataChannel: receiveDataChannel,
	}
}

// SendToKafka sends detected nearby users data to kafka.
func (un *UsersNearby) SendToKafka(userIDs []int64) error {
	// TODO: implement this.

	return nil
}
