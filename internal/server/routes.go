package server

import (
	"bluesky-oneshot-labeler/internal/config"

	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

func (s *FiberServer) RegisterFiberRoutes() {
	// Apply CORS middleware
	s.App.Use(cors.New(cors.Config{
		AllowOrigins:     "*",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS,PATCH",
		AllowHeaders:     "Accept,Authorization,Content-Type",
		AllowCredentials: false, // credentials require explicit origins
		MaxAge:           300,
	}))

	s.App.Get("/", s.HomeHandler)
	s.App.Get("/xrpc/com.atproto.label.queryLabels", s.QueryLabelsHandler)
	s.App.Get("/xrpc/com.atproto.label.subscribeLabels", websocket.New(s.SubscribeLabelsHandler))
	s.App.Get("/xrpc/*", s.NotImplementedHandler)
}

func (s *FiberServer) HomeHandler(c *fiber.Ctx) error {
	return c.Render("views/home", fiber.Map{
		"Upstream": config.UpstreamUser,
		"User":     config.Username,
	})
}

func (s *FiberServer) NotImplementedHandler(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(xrpc.XRPCError{
		ErrStr:  "MethodNotImplemented",
		Message: "Method not implemented",
	})
}
