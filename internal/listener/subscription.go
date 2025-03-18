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

func NewLabelNotifier(logger *slog.Logger) (*LabelNotifier, error) {
	db := database.Instance()
	latest, err := db.LatestLabelId()
	if err != nil {
		return nil, err
	}
	notifier := &LabelNotifier{
		subs: make([]*Subscriber, 0),
		log:  logger,
		db:   db,
	}
	notifier.last.Store(latest)
	return notifier, nil
}

func (ln *LabelNotifier) Notify(label *database.Label) {
	ln.lock.RLock()
	defer ln.lock.RUnlock()
	if len(ln.subs) == 0 {
		return
	}

	signed, err := SignRawLabel(label.Kind, label.Did, label.Cts, false)
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
	if err := SerializeEvent(event, &buf); err != nil {
		ln.log.Error("Failed to preserialize label", "error", err)
		return
	}
	event.Preserialized = buf.Bytes()
	ln.last.Store(label.Id)

	for _, sub := range ln.subs {
		sub.out <- event
	}
}

func SerializeEvent(event *events.XRPCStreamEvent, writer io.Writer) error {
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
		err := ln.forAllCatchUp(ctx, since, latest, fn)
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
	ln.log.Debug("caught up, starting subscription")
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

func (ln *LabelNotifier) forAllCatchUp(ctx context.Context, from, to int64, fn ForAllCallback) error {
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
		if err := ctx.Err(); err != nil {
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

var neg *bool

func SetNegation(negation *bool) {
	neg = negation
}

func SignRawLabel(kind int, did string, cts int64, profile bool) (*atproto.LabelDefs_Label, error) {
	var uri string
	if profile {
		uri = "at://did:" + did
	} else {
		uri = "did:" + did
	}
	unsigned := labels.UnsignedLabel{
		Cts: time.UnixMilli(cts).UTC().Format(time.RFC3339),
		Src: at_utils.UserDid.String(),
		// TODO: What on earth? `did:` is not a valid URI and the spec requires one,
		//   and yet Bluesky AppView expects it to be there?
		Uri: uri,
		Val: LabelKind(kind).String(),
		Ver: &at_utils.AtProtoVersion,
		Neg: neg,
	}
	return at_utils.SignLabel(&unsigned)
}
