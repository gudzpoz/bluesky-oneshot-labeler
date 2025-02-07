package server

import (
	"bluesky-oneshot-labeler/internal/config"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gofiber/fiber/v2"
)

type FiberServer struct {
	*fiber.App

	log *slog.Logger
}

func New(logger *slog.Logger) *FiberServer {
	server := &FiberServer{
		App: fiber.New(fiber.Config{
			ServerHeader: "bluesky-oneshot-labeler",
			AppName:      "bluesky-oneshot-labeler",
		}),
		log: logger,
	}

	return server
}

func (s *FiberServer) Run(ctx context.Context) {
	s.RegisterFiberRoutes()

	// Create a done channel to signal when the shutdown is complete
	done := make(chan bool, 1)

	go func() {
		port := config.Port
		err := s.Listen(fmt.Sprintf(":%d", port))
		if err != nil {
			panic(fmt.Sprintf("http server error: %s", err))
		}
	}()

	// Run graceful shutdown in a separate goroutine
	go s.gracefulShutdown(ctx, done)

	// Wait for the graceful shutdown to complete
	<-done
	s.log.Info("Graceful shutdown complete.")
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
