package server

import (
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"context"
	"embed"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
)

type FiberServer struct {
	*fiber.App

	db  *database.Service
	log *slog.Logger

	notifier *LabelNotifier
}

//go:embed views/*
var viewsFs embed.FS

func New(upstream *listener.LabelListener, logger *slog.Logger) *FiberServer {
	engine := html.NewFileSystem(http.FS(viewsFs), ".html")

	server := &FiberServer{
		App: fiber.New(fiber.Config{
			ServerHeader: "bluesky-oneshot-labeler",
			AppName:      "bluesky-oneshot-labeler",
			Views:        engine,
		}),
		db:  database.Instance(),
		log: logger,

		notifier: NewLabelNotifier(upstream, logger.WithGroup("notifier")),
	}

	return server
}

func (s *FiberServer) Run(ctx context.Context) chan bool {
	s.RegisterFiberRoutes()

	// Create a done channel to signal when the shutdown is complete
	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		port := fmt.Sprintf(":%d", config.Port)
		err := s.Listen(port)
		if err != nil {
			s.log.Error("http server error", "err", err)
		}
		cancel(err)
	}()

	// Run graceful shutdown in a separate goroutine
	done := make(chan bool, 1)
	go s.gracefulShutdown(ctx, done)
	return done
}

func (s *FiberServer) gracefulShutdown(ctx context.Context, done chan bool) {
	// Listen for the interrupt signal.
	<-ctx.Done()

	s.log.Info("shutting down gracefully, press Ctrl+C again to force")

	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.ShutdownWithContext(ctx); err != nil {
		s.log.Error("Server forced to shutdown with error: %v", "err", err)
	}

	s.log.Info("Server exiting")

	// Notify the main goroutine that the shutdown is complete
	done <- true
}
