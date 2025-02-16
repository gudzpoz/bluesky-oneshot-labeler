package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/database"
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/util/labels"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type Subscriber struct {
	out   chan *events.XRPCStreamEvent
	done  chan bool
	since int64
}

type LabelNotifier struct {
	db   *database.Service
	subs []*Subscriber
	lock sync.RWMutex
	last atomic.Int64
	log  *slog.Logger
}

func NewLabelNotifier(logger *slog.Logger) *LabelNotifier {
	notifier := &LabelNotifier{
		subs: make([]*Subscriber, 0),
		log:  logger,
		db:   database.Instance(),
	}
	return notifier
}

func (ln *LabelNotifier) Notify(label *database.Label) {
	ln.lock.RLock()
	defer ln.lock.RUnlock()
	if len(ln.subs) == 0 {
		return
	}

	signed, err := SignRawLabel(label.Kind, label.Did, label.Cts)
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
	if err := PreserializeEvent(event, &buf); err != nil {
		ln.log.Error("Failed to preserialize label", "error", err)
		return
	}
	event.Preserialized = buf.Bytes()
	ln.last.Store(label.Id)

	for _, sub := range ln.subs {
		sub.out <- event
	}
}

func PreserializeEvent(event *events.XRPCStreamEvent, writer io.Writer) error {
	w := cbg.NewCborWriter(writer)
	header := events.EventHeader{
		Op:      events.EvtKindMessage,
		MsgType: "#labels",
	}
	if err := header.MarshalCBOR(w); err != nil {
		return err
	}
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

type ForAllCallback func(*database.Label, *events.XRPCStreamEvent) error

func (ln *LabelNotifier) ForAllLabelsSince(
	ctx context.Context,
	since int64,
	fn ForAllCallback,
) error {
	sub := ln.Subscribe()
	latest := sub.since
	for latest > since {
		ln.Unsubscribe(sub)
		err := ln.forAllCatchUp(since, latest, fn)
		if err != nil {
			return err
		}
		since = latest
		select {
		case <-ctx.Done():
			return nil
		default:
			sub = ln.Subscribe()
		}
		latest = sub.since
	}
	defer ln.Unsubscribe(sub)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sub.done:
			return nil
		case event := <-sub.out:
			if err := fn(nil, event); err != nil {
				return nil
			}
		}
	}
}

func (ln *LabelNotifier) forAllCatchUp(from, to int64, fn ForAllCallback) error {
	ln.log.Debug("catching up subscription", "from", from, "to", to)

	rows, err := ln.db.QueryLabelsSince(from, to)
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
		if err := fn(&database.Label{
			Id:   id,
			Did:  did,
			Kind: kind,
			Cts:  cts,
		}, nil); err != nil {
			return err
		}
	}
	return nil
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

func SignRawLabel(kind int, did string, cts int64) (*atproto.LabelDefs_Label, error) {
	var val string
	switch kind {
	case LabelPorn:
		val = at_utils.LabelPornString
	case LabelSexual:
		val = at_utils.LabelSexualString
	case LabelNudity:
		val = at_utils.LabelNudityString
	case LabelGraphicMedia:
		val = at_utils.LabelGraphicMediaString
	case LabelOffender:
		val = "offender"
	default:
		val = "others"
	}
	unsigned := labels.UnsignedLabel{
		Cts: time.UnixMilli(cts).UTC().Format(time.RFC3339),
		Src: at_utils.UserDid.String(),
		Uri: "at://did:" + did,
		Val: val,
		Ver: &at_utils.AtProtoVersion,
	}
	return at_utils.SignLabel(&unsigned)
}
