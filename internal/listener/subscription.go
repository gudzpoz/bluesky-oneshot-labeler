package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/database"
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

type Block struct {
	Id         int64
	CompactDid string
}

type Subscriber struct {
	out   chan *Block
	done  chan bool
	since int64
}

type BlockNotifier struct {
	db   *database.Service
	subs []*Subscriber
	lock sync.RWMutex
	last atomic.Int64
	log  *slog.Logger
}

func NewBlockNotifier(logger *slog.Logger) (*BlockNotifier, error) {
	db := database.Instance()
	latest, err := db.LastBlockId()
	if err != nil {
		return nil, err
	}
	notifier := &BlockNotifier{
		subs: make([]*Subscriber, 0),
		log:  logger,
		db:   db,
	}
	notifier.last.Store(latest)
	return notifier, nil
}

func (ln *BlockNotifier) Notify(block *Block) {
	ln.lock.RLock()
	defer ln.lock.RUnlock()
	if len(ln.subs) == 0 {
		return
	}

	ln.last.Store(block.Id)

	for _, sub := range ln.subs {
		sub.out <- block
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

func (ln *BlockNotifier) Subscribe() *Subscriber {
	sub := &Subscriber{
		out:  make(chan *Block, 10),
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

func (ln *BlockNotifier) Unsubscribe(sub *Subscriber) {
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

type ForAllCallback func(block *Block, new bool) error

func (ln *BlockNotifier) ForAllLabelsSince(
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
			if err := fn(event, true); err != nil {
				return nil
			}
		}
	}
}

func (ln *BlockNotifier) forAllCatchUp(ctx context.Context, from, to int64, fn ForAllCallback) error {
	ln.log.Debug("catching up subscription", "from", from, "to", to)

	dids, lastId, err := ln.db.GetBlocksSince(from, to)
	if err != nil {
		return err
	}
	for _, did := range dids {
		if err := fn(&Block{
			Id:         lastId,
			CompactDid: did,
		}, false); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (ln *BlockNotifier) Close() {
	ln.lock.Lock()
	defer ln.lock.Unlock()
	for _, sub := range ln.subs {
		sub.done <- true
	}
	ln.subs = nil
	ln.last.Store(-1)
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
		Neg: nil,
	}
	return at_utils.SignLabel(&unsigned)
}
