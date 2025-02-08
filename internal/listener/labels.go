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

	_ "github.com/joho/godotenv/autoload"
)

const (
	LabelPornString         = "porn"
	LabelSexualString       = "sexual"
	LabelNudityString       = "nudity"
	LabelGraphicMediaString = "graphic-media"
)

const (
	LabelPorn = iota
	LabelSexual
	LabelNudity
	LabelGraphicMedia
	LabelOthers
	LabelOffender
)

type LabelListener struct {
	log *slog.Logger

	db        *database.Service
	serverUrl *url.URL
	labels    map[string]int

	cursor  atomic.Int64
	counter atomic.Int64

	offenderThreshold int64

	notifier func(*database.Label)
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

			offenderThreshold, err := db.GetConfigInt("offender-threshold", 0)
			if err != nil {
				return nil, err
			}
			if offenderThreshold == 0 {
				offenderThreshold = int64(config.OffenderThreshold)
				if err := db.SetConfigInt("offender-threshold", offenderThreshold); err != nil {
					return nil, err
				}
			}

			listener := &LabelListener{
				log:               logger,
				labels:            buildLabelMapping(details.Policies),
				serverUrl:         u,
				db:                db,
				offenderThreshold: offenderThreshold,
			}
			listener.cursor.Store(cursor)
			listener.counter.Store(counter)
			return listener, nil
		}
	}
	return nil, fmt.Errorf("labeler service not found")
}

func (l *LabelListener) Listen(ctx context.Context) chan bool {
	done := make(chan bool)
	go func() {
		for {
			l.log.Info("connecting in 1 second")
			select {
			case <-ctx.Done():
				l.log.Info("context done, listening stopped")
				done <- true
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

	stopPersist := l.startPersistSeq()

	err = events.HandleRepoStream(ctx, conn, scheduler, l.log)

	stopPersist <- true
	if err2 := l.persistSeq(); err2 != nil && err == nil {
		return err2
	}
	return err
}

func (l *LabelListener) SetNotifier(notifier func(*database.Label)) {
	l.notifier = notifier
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

		did, err := uriToDid(label.Uri)
		if err != nil {
			l.log.Warn("failed to parse label did", "uri", label.Uri, "err", err)
			continue
		}

		uid, err := l.db.GetUserId(did)
		if err != nil {
			l.log.Warn("failed to get user id", "did", did, "err", err)
			continue
		}

		when, err := time.Parse(time.RFC3339, label.Cts)
		if err != nil {
			when = time.Now().UTC()
		}
		whenMillis := when.UnixMilli()

		info, err := l.db.IncrementCounter(uid, kind, whenMillis)
		if err != nil {
			l.log.Warn("failed to increment counter", "kind", kind, "did", did, "err", err)
			continue
		}

		var notify bool
		if info.Count == 1 {
			notify = true
		} else if info.Count == l.offenderThreshold {
			info, err = l.db.IncrementCounter(uid, LabelOffender, whenMillis)
			if err != nil {
				l.log.Warn("failed to increment offender counter", "did", did, "err", err)
			}
			notify = true
			kind = LabelOffender
		} else {
			notify = false
		}

		if notify {
			l.notifier(&database.Label{
				Id:   info.Id,
				Did:  strings.TrimPrefix(did, "did:"),
				Kind: kind,
				Cts:  whenMillis,
			})
		}
	}
	at_utils.StoreLarger(&l.cursor, labels.Seq)
	l.counter.Add(1)
	return nil
}

func (l *LabelListener) startPersistSeq() chan bool {
	done := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Second):
				err := l.persistSeq()
				if err != nil {
					l.log.Warn("failed to persist label cursor", "err", err)
				}
			}
		}
	}()
	return done
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

func buildLabelMapping(policies *bsky.LabelerDefs_LabelerPolicies) map[string]int {
	m := make(map[string]int)
	m[LabelPornString] = LabelPorn
	m[LabelSexualString] = LabelSexual
	m[LabelNudityString] = LabelNudity
	m[LabelGraphicMediaString] = LabelGraphicMedia
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

func uriToDid(uri string) (string, error) {
	u, err := syntax.ParseATURI(uri)
	if err == nil {
		return u.Authority().String(), nil
	}

	if strings.HasPrefix(uri, "did:") {
		did, err := syntax.ParseDID(uri)
		if err != nil {
			return "", err
		}
		return did.String(), nil
	}

	return "", err
}
