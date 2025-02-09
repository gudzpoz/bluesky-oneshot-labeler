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
	if code := mainInner(); code != 0 {
		os.Exit(code)
	}
}

func mainInner() int {
	debug := flag.Bool("debug", false, "enable debug logging")
	publish := flag.Bool("publish", false, "publish labeler to user profile")
	flag.Parse()

	var level slog.Level
	if *debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}
	if err := initGlobals(level); err != nil {
		return 1
	}
	defer closeGlobals()

	var err error
	if *publish {
		err = publishLabeler()
	} else {
		err = runServer()
	}
	if err != nil {
		return 1
	}

	return 0
}

var background, startupCtx context.Context
var backgroundStop, startupStop context.CancelFunc
var logger *slog.Logger

func initGlobals(level slog.Level) error {
	slog.SetLogLoggerLevel(level)
	logger = slog.Default()

	background, backgroundStop = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	startupCtx, startupStop = context.WithTimeout(background, 10*time.Second)

	if err := database.InitDatabase(logger.WithGroup("database")); err != nil {
		logger.Error("failed to init database", "err", err)
		return err
	}

	if err := at_utils.InitKeys(); err != nil {
		logger.Error("failed to init keys", "err", err)
		return err
	}

	if err := at_utils.InitXrpcClient(startupCtx); err != nil {
		logger.Error("failed to init xrpc client", "err", err)
		return err
	}

	return nil
}

func closeGlobals() error {
	backgroundStop()
	startupStop()

	if err := database.Close(); err != nil {
		logger.Error("failed to close database", "err", err)
		return err
	}

	return nil
}

func publishLabeler() error {
	// if err := at_utils.PublishLabelerInfo(startupCtx); err != nil {
	// 	logger.Error("failed to publish labeler", "err", err)
	// 	return err
	// }

	if err := at_utils.PublishLabelInfo(startupCtx); err != nil {
		logger.Error("failed to publish labeler", "err", err)
		return err
	}

	return nil
}

func runServer() error {
	subscription, err := listener.NewLabelListener(startupCtx, logger)
	if err != nil {
		logger.Error("failed to create listener", "err", err)
		return err
	}

	server := server.New(subscription, logger)

	done := start(background, subscription, server)
	<-done

	return nil
}

func start(ctx context.Context, subscription *listener.LabelListener, server *server.FiberServer) chan bool {
	serverDone := server.Run(ctx)
	listenerDone := subscription.Listen(ctx)

	done := make(chan bool)

	go func() {
		<-listenerDone
		<-serverDone
		done <- true
	}()

	return done
}
