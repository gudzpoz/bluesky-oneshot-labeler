package main

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"bluesky-oneshot-labeler/internal/server"
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
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
	neg := flag.Bool("neg", false, "negate the labeler")
	publish := flag.Bool("publish", false, "publish labeler to user profile")
	rebuild := flag.Bool("rebuild", false, "rebuild label metadata")
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
	} else if *rebuild {
		err = rebuildLabels()
	} else {
		err = runServer(*neg)
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
	startupCtx, startupStop = context.WithTimeout(background, 30*time.Second)

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
	if err := at_utils.PublishLabelerInfo(background); err != nil {
		logger.Error("failed to publish labeler", "err", err)
		return err
	}

	if err := at_utils.PublishLabelInfo(background); err != nil {
		logger.Error("failed to publish label", "err", err)
		return err
	}

	if err := at_utils.PublishFeedInfo(background); err != nil {
		logger.Error("failed to publish feed", "err", err)
		return err
	}

	return nil
}

func rebuildLabels() error {
	subscription, err := listener.NewLabelListener(startupCtx, logger)
	if err != nil {
		logger.Error("failed to create listener", "err", err)
		return err
	}
	if err := subscription.RebuildLabels(); err != nil {
		logger.Error("failed to rebuild labels", "err", err)
		return err
	}
	return nil
}

type Runnable interface {
	Run(ctx context.Context) chan bool
}

func runServer(neg bool) error {
	subscription, err := listener.NewLabelListener(startupCtx, logger)
	if err != nil {
		logger.Error("failed to create listener", "err", err)
		return err
	}

	blockList, err := listener.NewBlockListInSync(config.ExternalBlockList, logger.WithGroup("csv"))
	if err != nil {
		logger.Error("failed to create block list", "err", err)
		return err
	}

	jetstream, err := listener.NewJetStreamListener(subscription, blockList, logger)
	if err != nil {
		logger.Error("failed to create jetstream listener", "err", err)
		return err
	}

	var negStart int64
	if neg {
		negStart, err = strconv.ParseInt(config.NegationStart, 10, 64)
		if err != nil {
			logger.Error("failed to parse negation start", "err", err)
			return err
		}
	}
	server := server.New(subscription, negStart, jetstream, logger)

	var done chan bool
	if neg {
		listener.SetNegation(&neg)
		done = start(background, blockList, jetstream, server)
	} else {
		done = start(background, subscription, blockList, jetstream, server)
	}
	<-done

	return nil
}

func start(ctx context.Context, runnables ...Runnable) chan bool {
	ctx, cancel := context.WithCancel(ctx)
	doneSignals := make([]chan bool, len(runnables))
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(runnables))
	for i, runnable := range runnables {
		doneSignals[i] = runnable.Run(ctx)
		go func() {
			<-doneSignals[i]
			logger.Info("done signal received", "service", fmt.Sprintf("%T", runnable))
			waitGroup.Done()
			cancel()
		}()
	}

	done := make(chan bool)
	go func() {
		waitGroup.Wait()
		cancel()
		done <- true
	}()

	return done
}
