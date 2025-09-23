package main

import (
	"log/slog"
	"love-signal-geo-data/internal/config"
	"love-signal-geo-data/pkg/logger"
)

func main() {
	cfg := config.MustLoad()

	log := logger.SetupLogger(cfg.Env)

	log.Info("starting application", slog.Any("config", cfg))
}
