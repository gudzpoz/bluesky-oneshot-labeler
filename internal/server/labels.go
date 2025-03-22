package server

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

// This file implements a no-op Bluesky labeler that provides no labels.
// The sole purpose of this labeler is to provide a convenient way to
// report posts for block list curation.

func (s *FiberServer) SubscribeLabelsHandler(c *websocket.Conn) {
	cursorStr := c.Query("cursor", "0")
	_, err := strconv.ParseInt(cursorStr, 10, 64)
	if err != nil {
		s.closeWithError(c, "InvalidRequest", "Invalid cursor")
		return
	}

	c.SetPingHandler(func(message string) error {
		err := c.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(time.Second*60))
		if err == websocket.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Timeout() {
			return nil
		}
		return err
	})
	c.SetPongHandler(func(_ string) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	c.SetCloseHandler(func(code int, text string) error { cancel(); return nil })
	<-ctx.Done()

	err = c.Close()
	if err != nil {
		s.log.Error("failed to close websocket", "error", err)
	}
}

func (s *FiberServer) closeWithError(c *websocket.Conn, errStr string, message string) {
	event := events.XRPCStreamEvent{
		Error: &events.ErrorFrame{
			Error:   errStr,
			Message: message,
		},
	}
	writer, err := c.NextWriter(websocket.BinaryMessage)
	if err == nil {
		if err := event.Serialize(writer); err != nil {
			s.log.Error("failed to serialize error event", "error", err)
		}
		if err := writer.Close(); err != nil {
			s.log.Error("failed to close error event writer", "error", err)
		}
	}
	if err := c.Close(); err != nil {
		s.log.Error("failed to close websocket", "error", err)
	}
}

type QueryLabelsInput struct {
	Cursor int64
	Limit  int64
}

func (s *FiberServer) QueryLabelsHandler(c *fiber.Ctx) error {
	input := QueryLabelsInput{
		Cursor: 0,
		Limit:  10,
	}
	err := c.QueryParser(&input)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: err.Error(),
		})
	}

	if input.Limit <= 0 || 250 < input.Limit {
		return c.Status(fiber.StatusBadRequest).JSON(xrpc.XRPCError{
			ErrStr:  "InvalidRequest",
			Message: "limit out of range (0 < limit <= 250)",
		})
	}

	return c.JSON(atproto.LabelQueryLabels_Output{
		Labels: []*atproto.LabelDefs_Label{},
	})
}
