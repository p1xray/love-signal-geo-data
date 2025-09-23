package app

import (
	"context"
	"log/slog"
	"love-signal-geo-data/internal/app/kafka"
	"love-signal-geo-data/internal/config"
	"love-signal-geo-data/internal/infrastructure/kafka/handlers"
	"os"
	"os/signal"
	"syscall"
)

// App is an application.
type App struct {
	log      *slog.Logger
	kafkaApp *kafka.App
}

// New creates a new application.
func New(
	log *slog.Logger,
	cfg *config.Config,
) *App {
	// Handlers.
	userCoordinatesHandler := handlers.NewUserCoordinates(log)

	// Apps.
	kafkaApp := kafka.New(log, cfg.Kafka, userCoordinatesHandler)

	return &App{
		log:      log,
		kafkaApp: kafkaApp,
	}
}

// Start - starts the application.
func (a *App) Start(ctx context.Context) {
	const op = "app.Start"

	log := a.log.With(slog.String("op", op))
	log.Info("starting application")

	a.kafkaApp.Start(ctx)
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
}
