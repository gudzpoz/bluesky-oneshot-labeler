package server

import (
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"context"
	"net"
	"strconv"
	"time"

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

	var diff int64
	cursor, diff = s.convertCursor(cursor)
	if latest < cursor {
		s.closeWithError(c, "FutureCursor", "Cursor is in the future")
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
	if s.labelNegStart != 0 {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if diff < 2*s.labelNegStart {
			err = s.notifier.ForAllLabelsSince(canceled, cursor, s.writeLabelAsEventFunc(c, false, s.labelNegStart))
		}
		if err == nil {
			err = s.notifier.ForAllLabelsSince(ctx, cursor, s.writeLabelAsEventFunc(c, true, 2*s.labelNegStart))
		}
	} else {
		err = s.notifier.ForAllLabelsSince(ctx, cursor, s.writeLabelAsEventFunc(c, false, diff))
	}
	if err != nil {
		s.closeWithError(c, "InternalError", err.Error())
		return
	}

	err = c.Close()
	if err != nil {
		s.log.Error("failed to close websocket", "error", err)
	}
}

func (s *FiberServer) writeLabelAsEventFunc(c *websocket.Conn, profile bool, diff int64) func(l *database.Label, xe *events.XRPCStreamEvent) error {
	return func(l *database.Label, xe *events.XRPCStreamEvent) error {
		if xe == nil {
			signed, err := listener.SignRawLabel(l.Kind, l.Did, l.Cts, profile)
			if err != nil {
				return err
			}
			xe = &events.XRPCStreamEvent{
				LabelLabels: &atproto.LabelSubscribeLabels_Labels{
					Labels: []*atproto.LabelDefs_Label{signed},
					Seq:    l.Id + diff,
				},
			}
		}
		return s.writeEvent(c, xe)
	}
}

func (s *FiberServer) writeEvent(c *websocket.Conn, event *events.XRPCStreamEvent) error {
	writer, err := c.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if event.Preserialized == nil {
		err = listener.SerializeEvent(event, writer)
	} else {
		_, err = writer.Write(event.Preserialized)
	}
	if err != nil {
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
