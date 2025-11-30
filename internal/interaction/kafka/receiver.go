package kafka

import (
	"context"
	"log/slog"
	"love-signal-geo-data/pkg/kafka"
	"love-signal-geo-data/pkg/logger/sl"
)

type Receiver struct {
	log    *slog.Logger
	output <-chan kafka.Message

	processUserCoordinates UserCoordinatesUseCase
}

func NewReceiver(
	log *slog.Logger,
	output <-chan kafka.Message,
	processUserCoordinates UserCoordinatesUseCase,
) *Receiver {
	return &Receiver{
		log:    log,
		output: output,

		processUserCoordinates: processUserCoordinates,
	}
}

func (r *Receiver) Receive(ctx context.Context) {
	const op = "interaction.kafka.receiver.Receive"

	log := r.log.With(
		slog.String("op", op),
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-r.output:
				log.Info("received message from kafka", slog.String("topic", msg.Topic))

				r.processMessage(ctx, log, msg)
			default:
			}
		}
	}()
}

func (r *Receiver) processMessage(ctx context.Context, log *slog.Logger, msg kafka.Message) {
	switch msg.Topic {
	case "":
		return
	case UserCoordinatesTopic:
		go func() {
			if err := r.processUserCoordinates.Execute(ctx, msg.Data); err != nil {
				log.Error("error handling new user coordinates from kafka", sl.Err(err))
			}
		}()
	default:
		log.Warn("handler implementation for topic does not exist", slog.String("topic", msg.Topic))
	}
}
