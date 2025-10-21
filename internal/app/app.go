package app

import (
	"context"
	"log/slog"
	"love-signal-geo-data/internal/app/kafka"
	"love-signal-geo-data/internal/config"
	infrKafka "love-signal-geo-data/internal/infrastructure/kafka"
	"love-signal-geo-data/internal/infrastructure/kafka/handlers"
	"love-signal-geo-data/internal/infrastructure/repository"
	"love-signal-geo-data/internal/infrastructure/storage/postgresql"
	"love-signal-geo-data/internal/usecase/coordinates"
	"love-signal-geo-data/pkg/logger/sl"
	"os"
	"os/signal"
	"syscall"
)

// App is an application.
type App struct {
	log                    *slog.Logger
	kafkaApp               *kafka.App
	storage                *postgresql.Storage
	processUserCoordinates *coordinates.UseCase
}

// New creates a new application.
func New(
	log *slog.Logger,
	cfg *config.Config,
) *App {
	// Storages.
	storage, err := postgresql.New(cfg.PostgreSQL)
	if err != nil {
		log.Error("error connecting to the PostgreSQL database", sl.Err(err))

		panic(err)
	}

	// Repositories.
	coordinatesStorage := repository.NewCoordinatesRepository(log, storage)

	// Apps.
	kafkaApp := kafka.New(log, cfg.Kafka)

	// Handlers.
	usersNearbyHandler := handlers.NewUsersNearby(log, kafkaApp.Input())

	// Use-cases.
	processUserCoordinates := coordinates.New(log, coordinatesStorage, usersNearbyHandler)

	return &App{
		log:                    log,
		kafkaApp:               kafkaApp,
		storage:                storage,
		processUserCoordinates: processUserCoordinates,
	}
}

// Start - starts the application.
func (a *App) Start(ctx context.Context) {
	const op = "app.Start"

	log := a.log.With(slog.String("op", op))
	log.Info("starting application")

	a.kafkaApp.Start(ctx)

	go func() {
		for {
			select {
			case msg := <-a.kafkaApp.Output():
				log.Info("received message from kafka", slog.String("topic", msg.Topic))

				switch msg.Topic {
				case infrKafka.UserCoordinatesTopic:
					go func() {
						if err := a.processUserCoordinates.Execute(ctx, msg.Data); err != nil {
							log.Error("error handling new user coordinates from kafka", sl.Err(err))
						}
					}()
				default:
					log.Warn("handler implementation for topic does not exist", slog.String("topic", msg.Topic))
				}
			default:
			}
		}
	}()
}

// GracefulStop - gracefully stops the application.
func (a *App) GracefulStop() {
	const op = "app.GracefulStop"

	log := a.log.With(slog.String("op", op))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	select {
	case s := <-stop:
		log.Info("signal received from OS", slog.String("signal:", s.String()))
	}

	log.Info("stopping application")

	a.kafkaApp.Stop()
	a.storage.Close()
}
