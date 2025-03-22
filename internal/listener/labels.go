package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
)

type LabelKind int

const (
	LabelPorn LabelKind = iota
	LabelSexual
	LabelNudity
	LabelGraphicMedia
	LabelOthers
)

func (l LabelKind) String() (val string) {
	switch l {
	case LabelPorn:
		val = at_utils.LabelPornString
	case LabelSexual:
		val = at_utils.LabelSexualString
	case LabelNudity:
		val = at_utils.LabelNudityString
	case LabelGraphicMedia:
		val = at_utils.LabelGraphicMediaString
	default:
		val = "others"
	}
	return
}

type LabelListener struct {
	log *slog.Logger

	db        *database.Service
	serverUrl *url.URL
	labels    map[string]LabelKind

	cursor  atomic.Int64
	counter atomic.Int64

	watcher *AccountWatcher
}

func NewLabelListener(ctx context.Context, logger *slog.Logger) (*LabelListener, error) {
	labeler, _ := syntax.ParseHandle(config.UpstreamUser)
	ident, err := at_utils.IdentityDirectory.LookupHandle(ctx, labeler)
	if err != nil {
		return nil, err
	}

	db := database.Instance()
	cursor, err := db.GetConfigInt("label-cursor", 0)
	if err != nil {
		return nil, err
	}
	counter, err := db.GetConfigInt("label-counter", 0)
	if err != nil {
		return nil, err
	}

	logger.Debug(config.UpstreamUser, "ident", ident)
	for _, value := range ident.Services {
		if value.Type == "AtprotoLabeler" {
			u, err := url.Parse(value.URL)
			if err != nil {
				return nil, err
			}

			info, err := bsky.LabelerGetServices(ctx, at_utils.Client, true, []string{ident.DID.String()})
			if err != nil {
				return nil, err
			}
			if len(info.Views) != 1 {
				return nil, fmt.Errorf("expected one service view, got %d", len(info.Views))
			}
			view := info.Views[0]
			details := view.LabelerDefs_LabelerViewDetailed
			if details == nil {
				return nil, fmt.Errorf("labeler service view is not detailed")
			}

			watcher, err := NewAccountWatcher(logger)
			if err != nil {
				return nil, err
			}

			listener := &LabelListener{
				log:       logger,
				labels:    buildLabelMapping(details.Policies),
				serverUrl: u,
				db:        db,
				watcher:   watcher,
			}
			listener.cursor.Store(cursor)
			listener.counter.Store(counter)
			return listener, nil
		}
	}
	return nil, fmt.Errorf("labeler service not found")
}

func (l *LabelListener) Run(ctx context.Context) chan bool {
	watcherCtx, stopWatcher := context.WithCancel(context.Background())
	go l.watcher.Listen(watcherCtx)

	done := make(chan bool)
	go func() {
		for {
			l.log.Info("connecting in 1 second")
			select {
			case <-ctx.Done():
				l.log.Info("context done, label listening stopped")
				done <- true
				stopWatcher()
				return
			case <-time.After(1 * time.Second):
				if err := l.listen(ctx); err != nil {
					l.log.Error("failed to listen", "err", err)
				}
				l.log.Info("websocket disconnected")
			}
		}
	}()
	return done
}

func (l *LabelListener) listen(ctx context.Context) error {
	u, _ := url.Parse("wss://example.com/xrpc/com.atproto.label.subscribeLabels")
	u.Host = l.serverUrl.Host
	u.RawQuery = fmt.Sprintf("cursor=%d", l.cursor.Load())

	scheduler := sequential.NewScheduler("oneshot-labeler", l.HandleEvent)
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	go l.startPersistSeq(ctx)
	err = events.HandleRepoStream(ctx, conn, scheduler, l.log)

	if err2 := l.persistSeq(); err2 != nil && err == nil {
		return err2
	}
	return err
}

func (l *LabelListener) Notifier() *BlockNotifier {
	return l.watcher.notifier
}

func (l *LabelListener) HandleEvent(ctx context.Context, event *events.XRPCStreamEvent) error {
	labels := event.LabelLabels
	if labels == nil {
		return nil
	}
	for _, label := range labels.Labels {
		if label.Neg != nil && *label.Neg {
			continue
		}
		kind, ok := l.labels[label.Val]
		if !ok {
			continue
		}

		info := explainLabel(label.Uri)
		switch info.Kind {
		case LabelUnknown:
			l.log.Warn("failed to parse label did", "info", info)
			continue
		case LabelOnProfile:
			l.log.Warn("ignores label on profile", "user", info.Did)
			continue
		case LabelOnPost:
		case LabelOnUser:

		}

		uid, err := l.db.GetUserId(info.Did)
		if err != nil {
			l.log.Warn("failed to get user id", "did", info.Did, "err", err)
			continue
		}

		id, count, err := l.db.IncrementCounter(uid, int(kind))
		if err != nil {
			l.log.Warn("failed to increment counter", "kind", kind, "did", info.Did, "err", err)
			continue
		}

		l.watcher.CheckAccount(id, info.Did, count)
	}
	at_utils.StoreLarger(&l.cursor, labels.Seq)
	l.counter.Add(1)
	return nil
}

func (l *LabelListener) startPersistSeq(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Minute):
			err := l.persistSeq()
			if err != nil {
				l.log.Warn("failed to persist label cursor", "err", err)
			}
		}
	}
}

func (l *LabelListener) persistSeq() error {
	cursor := l.cursor.Load()
	counter := l.counter.Load()
	l.log.Debug("persisting label cursor", "cursor", cursor, "counter", counter)
	if err := l.db.SetConfigInt("label-cursor", cursor); err != nil {
		return err
	}
	if err := l.db.SetConfigInt("label-counter", counter); err != nil {
		return err
	}
	return nil
}

func buildLabelMapping(policies *bsky.LabelerDefs_LabelerPolicies) map[string]LabelKind {
	m := make(map[string]LabelKind)
	m[at_utils.LabelPornString] = LabelPorn
	m[at_utils.LabelSexualString] = LabelSexual
	m[at_utils.LabelNudityString] = LabelNudity
	m[at_utils.LabelGraphicMediaString] = LabelGraphicMedia
	for _, policy := range policies.LabelValueDefinitions {
		if policy.AdultOnly != nil && *policy.AdultOnly {
			m[policy.Identifier] = LabelSexual
		} else if policy.Blurs != "none" {
			m[policy.Identifier] = LabelGraphicMedia
		} else {
			m[policy.Identifier] = LabelOthers
		}
	}
	return m
}

type LabelIntention int

const (
	LabelOnUser LabelIntention = iota
	LabelOnPost
	LabelOnProfile
	LabelUnknown
)

type LabelExplained struct {
	Did  string
	RKey string
	Kind LabelIntention
}

func explainLabel(uri string) LabelExplained {
	u, err := syntax.ParseATURI(uri)
	if err == nil {
		rkey := u.RecordKey().String()
		if rkey == "" {
			rkey = u.Path()
		}
		did := u.Authority().String()
		reason := LabelOnPost
		if u.Collection().String() != "app.bsky.feed.post" {
			if u.Path() == "" {
				reason = LabelOnProfile
			} else {
				reason = LabelUnknown
			}
		}
		return LabelExplained{
			Did:  did,
			RKey: rkey,
			Kind: reason,
		}
	}

	if strings.HasPrefix(uri, "did:") {
		did, err := syntax.ParseDID(uri)
		if err != nil {
			return LabelExplained{
				RKey: uri,
				Kind: LabelUnknown,
			}
		}
		return LabelExplained{
			Did:  did.String(),
			Kind: LabelOnUser,
		}
	}

	return LabelExplained{
		RKey: uri,
		Kind: LabelUnknown,
	}
}
