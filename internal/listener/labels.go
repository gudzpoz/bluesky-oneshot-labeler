package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
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
	LabelOffender
)

type LabelListener struct {
	log *slog.Logger

	db        *database.Service
	serverUrl *url.URL
	labels    map[string]int

	cursor  atomic.Int64
	counter atomic.Int64

	blockThreshold    int
	offenderThreshold int
}

func NewLabelListener(ctx context.Context, logger *slog.Logger) (*LabelListener, error) {
	labeler, _ := syntax.ParseHandle("moderation.bsky.app")
	ident, err := at_utils.IdentityDirectory.LookupHandle(ctx, labeler)
	if err != nil {
		return nil, err
	}

	db := database.Instance()
	cursorStr, err := db.GetConfig("label-cursor", "0")
	if err != nil {
		return nil, err
	}
	cursor, err := strconv.ParseInt(cursorStr, 10, 64)

	logger.Debug("moderation.bsky.app", "ident", ident)
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

			blockThreshold, err := strconv.Atoi(config.BlockThreshold)
			if err != nil {
				return nil, err
			}
			offenderThreshold, err := strconv.Atoi(config.OffenderThreshold)
			if err != nil {
				return nil, err
			}

			listener := &LabelListener{
				log:               logger,
				labels:            buildLabelMapping(details.Policies),
				serverUrl:         u,
				db:                db,
				blockThreshold:    blockThreshold,
				offenderThreshold: offenderThreshold,
			}
			listener.cursor.Store(cursor)
			return listener, nil
		}
	}
	return nil, fmt.Errorf("labeler service not found")
}

func (l *LabelListener) Listen(ctx context.Context) error {
	u, _ := url.Parse("wss://example.com/xrpc/com.atproto.label.subscribeLabels")
	u.Host = l.serverUrl.Host
	u.RawQuery = fmt.Sprintf("cursor=%d", l.cursor.Load())

	scheduler := parallel.NewScheduler(16, 4096, "oneshot-labeler", l.HandleEvent)
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	stopPersist := l.startPersistSeq()

	err = events.HandleRepoStream(ctx, conn, scheduler, l.log)

	stopPersist <- true
	if err2 := l.db.SetConfig("label-cursor", strconv.FormatInt(l.cursor.Load(), 10)); err2 != nil {
		l.log.Warn("failed to set label cursor", "err", err2)
		if err != nil {
			return err
		}
		return err2
	}
	return err
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

		count, err := l.db.IncrementCounter(kind, did)
		if err != nil {
			l.log.Warn("failed to increment counter", "kind", kind, "did", did, "err", err)
			continue
		}

		when, err := time.Parse(time.RFC3339, label.Cts)
		if err != nil {
			when = time.Now()
		}
		whenMillis := when.UnixMilli()

		if count == 1 {
			err = l.db.BlockUser(kind, did, whenMillis)
		} else if count == 5 {
			err = l.db.BlockUser(LabelOffender, did, whenMillis)
		} else {
			continue
		}
		if err != nil {
			l.log.Warn("failed to block user", "kind", kind, "did", did, "err", err)
			continue
		}

		// TODO: Notify websockets: newly blocked users
	}
	l.cursor.Store(labels.Seq)
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
			case <-time.After(30 * time.Second):
				cursor := l.cursor.Load()
				counter := l.counter.Load()
				l.log.Info("persisting label cursor", "cursor", cursor, "counter", counter)
				if err := l.db.SetConfig("label-cursor", strconv.FormatInt(cursor, 10)); err != nil {
					l.log.Warn("failed to set label cursor", "err", err)
				}
			}
		}
	}()
	return done
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
