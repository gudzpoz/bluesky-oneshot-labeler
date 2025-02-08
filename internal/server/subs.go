package server

import (
	"bluesky-oneshot-labeler/internal/database"
	"bluesky-oneshot-labeler/internal/listener"
	"bytes"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/gofiber/contrib/websocket"

	cbg "github.com/whyrusleeping/cbor-gen"
)

type Subscriber struct {
	out   chan *events.XRPCStreamEvent
	done  chan bool
	since int64
}

type LabelNotifier struct {
	subs []*Subscriber
	lock sync.RWMutex
	last atomic.Int64
	log  *slog.Logger
}

func NewLabelNotifier(upstream *listener.LabelListener, logger *slog.Logger) *LabelNotifier {
	notifier := &LabelNotifier{
		subs: make([]*Subscriber, 0),
		log:  logger,
	}
	upstream.SetNotifier(notifier.Notify)
	return notifier
}

func (ln *LabelNotifier) Notify(label *database.Label) {
	ln.lock.RLock()
	defer ln.lock.RUnlock()
	if len(ln.subs) == 0 {
		return
	}

	signed, err := signLabel(label.Kind, label.Did, label.Cts)
	if err != nil {
		ln.log.Error("Failed to sign label", "error", err)
		return
	}
	event := &events.XRPCStreamEvent{
		LabelLabels: &atproto.LabelSubscribeLabels_Labels{
			Labels: []*atproto.LabelDefs_Label{signed},
			Seq:    label.Id,
		},
	}

	// event.Preserialize() does not support LabelLabels yet
	var buf bytes.Buffer
	if err := serialize(event, &buf); err != nil {
		ln.log.Error("Failed to preserialize label", "error", err)
		return
	}
	event.Preserialized = buf.Bytes()
	ln.last.Store(label.Id)

	for _, sub := range ln.subs {
		sub.out <- event
	}
}

func serialize(event *events.XRPCStreamEvent, writer io.Writer) error {
	w := cbg.NewCborWriter(writer)
	if err := event.LabelLabels.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (ln *LabelNotifier) Subscribe() *Subscriber {
	sub := &Subscriber{
		out:  make(chan *events.XRPCStreamEvent, 10),
		done: make(chan bool),
	}
	ln.lock.Lock()
	defer ln.lock.Unlock()
	ln.subs = append(ln.subs, sub)
	sub.since = ln.last.Load()
	if sub.since < 0 {
		// closed
		sub.done <- true
	}
	return sub
}

func (ln *LabelNotifier) Unsubscribe(sub *Subscriber) {
	ln.lock.Lock()
	defer ln.lock.Unlock()
	for i, s := range ln.subs {
		if s == sub {
			ln.subs[i] = ln.subs[len(ln.subs)-1]
			ln.subs = ln.subs[:len(ln.subs)-1]
			break
		}
	}
}

func (ln *LabelNotifier) Close() {
	ln.lock.Lock()
	defer ln.lock.Unlock()
	for _, sub := range ln.subs {
		sub.done <- true
	}
	ln.subs = nil
	ln.last.Store(-1)
}

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

	var sub *Subscriber
	for {
		if latest > cursor {
			err := s.subscriptionCatchUp(c, cursor)
			if err != nil {
				s.closeWithError(c, "InternalError", err.Error())
				return
			}
			cursor = latest
		}
		sub = s.notifier.Subscribe()
		latest = sub.since
		if latest <= cursor {
			break
		}
		s.notifier.Unsubscribe(sub)
	}
	defer s.notifier.Unsubscribe(sub)

	for {
		select {
		case <-sub.done:
			c.Close()
			return
		case event := <-sub.out:
			if err := s.writeEvent(c, event); err != nil {
				s.closeWithError(c, "InternalError", err.Error())
				return
			}
		}
	}
}

func (s *FiberServer) subscriptionCatchUp(c *websocket.Conn, cursor int64) error {
	s.log.Debug("catching up subscription", "cursor", cursor)

	rows, err := s.db.QueryLabelsSince(cursor)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var did string
		var kind int
		var cts int64
		err := rows.Scan(&id, &did, &kind, &cts)
		if err != nil {
			return err
		}
		signed, err := signLabel(kind, did, cts)
		if err != nil {
			return err
		}
		event := events.XRPCStreamEvent{
			LabelLabels: &atproto.LabelSubscribeLabels_Labels{
				Labels: []*atproto.LabelDefs_Label{signed},
				Seq:    id,
			},
		}
		if err := s.writeEvent(c, &event); err != nil {
			return err
		}
	}
	return nil
}

func (s *FiberServer) writeEvent(c *websocket.Conn, event *events.XRPCStreamEvent) error {
	writer, err := c.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if err := serialize(event, writer); err != nil {
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
