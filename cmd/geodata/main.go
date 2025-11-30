package main

import (
	"context"
	"log/slog"
	"love-signal-geo-data/internal/app"
	"love-signal-geo-data/internal/config"
	"love-signal-geo-data/pkg/logger"
)

func main() {
	cfg := config.MustLoad()

	log := logger.SetupLogger(cfg.Env)

	log.Info("starting application", slog.Any("config", cfg))

	application := app.New(log, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		application.Start(ctx)
	}()

	application.GracefulStop(cancel)
	log.Info("application stopped")
}
