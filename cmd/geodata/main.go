package main

import (
	"fmt"
	"love-signal-geo-data/internal/config"
)

func main() {
	cfg := config.MustLoad()

	fmt.Printf("cfg: %v", cfg)
}
