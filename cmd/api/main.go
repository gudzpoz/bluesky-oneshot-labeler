package main

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"bluesky-oneshot-labeler/internal/server"
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	background, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	startupCtx, cancel := context.WithTimeout(background, 10*time.Second)
	defer cancel()

	err := database.Init(logger.WithGroup("database"))
	if err != nil {
		logger.Error("failed to init database", "err", err)
		os.Exit(1)
	}

	err = at_utils.InitXrpcClient(startupCtx)
	if err != nil {
		logger.Error("failed to init xrpc client", "err", err)
		os.Exit(1)
	}

	subscription, err := listener.NewLabelListener(startupCtx, logger)
	if err != nil {
		logger.Error("failed to create listener", "err", err)
		os.Exit(1)
	}

	err = subscription.Listen(background)
	if err != nil {
		logger.Error("listener error", "err", err)
		os.Exit(1)
	}

	server := server.New(logger)
	server.Run(background)

}
