package kafka

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"love-signal-geo-data/pkg/kafka"
	"love-signal-geo-data/pkg/logger/sl"
)

type Sender struct {
	log   *slog.Logger
	input chan<- kafka.Message
}

func NewSender(log *slog.Logger, input chan<- kafka.Message) *Sender {
	return &Sender{
		log:   log,
		input: input,
	}
}

func (s *Sender) Send(topic string, data any) error {
	const op = "kafka.Sender.Send"

	log := s.log.With(
		slog.String("op", op),
		slog.String("topic", topic),
		slog.Any("data", data),
	)

	jsonKafkaDataAsByte, err := json.Marshal(data)
	if err != nil {
		log.Error("error marshaling data for sending to kafka", sl.Err(err))

		return fmt.Errorf("%w: %w", ErrMarshalData, err)
	}

	kafkaMessage := kafka.Message{
		Topic: topic,
		Data:  jsonKafkaDataAsByte,
	}
	s.input <- kafkaMessage

	return nil
}
