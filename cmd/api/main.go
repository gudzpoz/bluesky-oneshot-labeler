package main

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/listener"
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	_ "github.com/joho/godotenv/autoload"
)

func main() {

	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	var level slog.Level
	if *debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))

	startupCtx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	err := at_utils.InitXrpcClient(startupCtx)
	if err != nil {
		logger.Error("failed to init xrpc client", "err", err)
		return
	}

	subscription, err := listener.NewLabelListener(startupCtx, logger)
	if err != nil {
		logger.Error("failed to create listener", "err", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	subscription.Listen(ctx)

	// server := server.New(logger)
	// server.Run()

}
