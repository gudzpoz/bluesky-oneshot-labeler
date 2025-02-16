package server

import (
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"context"
	"strconv"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/gofiber/contrib/websocket"
)

func (s *FiberServer) SubscribeLabelsHandler(c *websocket.Conn) {
	cursorStr := c.Query("cursor", "0")
	cursor, err := strconv.ParseInt(cursorStr, 10, 64)
	if err != nil {
		s.closeWithError(c, "InvalidRequest", "Invalid cursor")
		return
	}
	latest, err := s.db.LatestLabelId()
	if err != nil {
		s.closeWithError(c, "InternalError", err.Error())
		return
	}
	if latest < cursor {
		s.closeWithError(c, "FutureCursor", "Cursor is in the future")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.SetCloseHandler(func(code int, text string) error { cancel(); return nil })
	err = s.notifier.ForAllLabelsSince(ctx, cursor, func(l *database.Label, xe *events.XRPCStreamEvent) error {
		if xe == nil {
			signed, err := listener.SignRawLabel(l.Kind, l.Did, l.Cts)
			if err != nil {
				return err
			}
			xe = &events.XRPCStreamEvent{
				LabelLabels: &atproto.LabelSubscribeLabels_Labels{
					Labels: []*atproto.LabelDefs_Label{signed},
					Seq:    l.Id,
				},
			}
		}
		return s.writeEvent(c, xe)
	})
	if err != nil {
		s.closeWithError(c, "InternalError", err.Error())
		return
	}

	err = c.Close()
	if err != nil {
		s.log.Error("failed to close websocket", "error", err)
	}
}

func (s *FiberServer) writeEvent(c *websocket.Conn, event *events.XRPCStreamEvent) error {
	writer, err := c.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if _, err := writer.Write(event.Preserialized); err != nil {
		s.log.Error("failed to serialize event", "error", err)
	}
	return writer.Close()
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
