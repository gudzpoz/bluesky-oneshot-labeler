package main

import (
	"bluesky-oneshot-labeler/internal/server"
	"log/slog"
	"os"

	_ "github.com/joho/godotenv/autoload"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	server := server.New(logger)
	server.Run()

}
