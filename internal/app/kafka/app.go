package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"love-signal-geo-data/internal/config"
	"love-signal-geo-data/pkg/kafka"
	"love-signal-geo-data/pkg/logger/sl"
	"strings"
)

// App is a kafka queue application.
type App struct {
	log       *slog.Logger
	producer  *kafka.Producer
	consumers []*kafka.Consumer
	input     chan kafka.Message
}

// New returns new instance of kafka queue application.
func New(
	log *slog.Logger,
	cfg config.KafkaConfig,
) *App {
	address := strings.Split(cfg.Address, ",")

	producer := kafka.NewAsyncProducer(address, kafka.AcksRequireAll())

	return &App{
		log:       log,
		producer:  producer,
		consumers: []*kafka.Consumer{},
		input:     make(chan kafka.Message),
	}
}

// Start - starts the kafka queue application.
func (a *App) Start(ctx context.Context) {
	const op = "kafka.app.Start"

	log := a.log.With(
		slog.String("op", op),
	)

	log.Info(fmt.Sprintf("running kafka %d consumers", len(a.consumers)))
	a.startConsumers(ctx)

	log.Info("running kafka producer")
	a.startProducer()
}

// Input is the input channel for the user to write messages to that they wish to send.
func (a *App) Input() chan<- kafka.Message {
	return a.input
}

// Stop - stops the kafka queue application.
func (a *App) Stop() {
	const op = "kafka.app.Stop"

	log := a.log.With(
		slog.String("op", op),
	)
	log.Info("stopping kafka consumers")
	a.stopConsumers(log)

	log.Info("stopping kafka producer")
	a.stopProducer(log)
}

func (a *App) startConsumers(ctx context.Context) {
	for _, c := range a.consumers {
		go c.Consume(ctx)
	}

	a.handleConsumerReceivedMessages(ctx)
	a.handleConsumerErrors()
}

func (a *App) stopConsumers(log *slog.Logger) {
	for _, consumer := range a.consumers {
		if err := consumer.Close(); err != nil {
			log.Error("error closing kafka consumer", sl.Err(err))
		}
	}
}

func (a *App) startProducer() {
	go func() {
		for d := range a.input {
			var key []byte
			if len(d.Key) > 0 {
				key = []byte(d.Key)
			}

			a.producer.ProduceAsync(d.Topic, key, d.Data)
		}
	}()

	a.handleAsyncProducerErrors()
}

func (a *App) stopProducer(log *slog.Logger) {
	defer close(a.input)

	if err := a.producer.Close(); err != nil {
		log.Error("error closing kafka producer", sl.Err(err))
	}
}

func (a *App) handleConsumerReceivedMessages(ctx context.Context) {
	const op = "kafka.app.handleConsumerReceivedMessages"

	log := a.log.With(slog.String("op", op))

	for _, consumer := range a.consumers {
		go func() {
			for {
				select {
				case msg := <-consumer.Output():
					log.Info("received message from kafka", slog.String("topic", msg.Topic))

					switch msg.Topic {
					default:
						log.Warn("handler implementation for topic does not exist", slog.String("topic", msg.Topic))
					}
				default:
				}
			}
		}()
	}
}

func (a *App) handleConsumerErrors() {
	const op = "kafka.app.handleConsumerErrors"

	log := a.log.With(slog.String("op", op))

	for _, c := range a.consumers {
		go func() {
			for {
				select {
				case err := <-c.Notify():
					if err != nil {
						log.Error("error reading message from kafka", sl.Err(err))
					}
				default:
				}
			}
		}()
	}
}

func (a *App) handleAsyncProducerErrors() {
	const op = "kafka.app.handleAsyncProducerErrors"

	log := a.log.With(slog.String("op", op))

	go func() {
		for {
			select {
			case err := <-a.producer.Notify():
				if err != nil {
					log.Error("error writing message to kafka", sl.Err(err))
				}
			default:
			}
		}
	}()
}
