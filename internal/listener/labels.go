package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/gorilla/websocket"
)

const (
	LabelPorn         = "porn"
	LabelSexual       = "sexual"
	LabelNudity       = "nudity"
	LabelGraphicMedia = "graphic-media"
)

type LabelListener struct {
	log *slog.Logger

	labels    map[string]string
	serverUrl *url.URL
}

func NewLabelListener(ctx context.Context, logger *slog.Logger) (*LabelListener, error) {
	labeler, _ := syntax.ParseHandle("moderation.bsky.app")
	ident, err := at_utils.IdentityDirectory.LookupHandle(ctx, labeler)
	if err != nil {
		return nil, err
	}

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

			return &LabelListener{
				log:       logger,
				labels:    buildLabelMapping(details.Policies),
				serverUrl: u,
			}, nil
		}
	}
	return nil, fmt.Errorf("labeler service not found")
}

func (l *LabelListener) Listen(ctx context.Context) error {
	u, _ := url.Parse("wss://example.com/xrpc/com.atproto.label.subscribeLabels")
	u.Host = l.serverUrl.Host
	u.RawQuery = "cursor=0"

	scheduler := parallel.NewScheduler(16, 4096, "oneshot-labeler", l.HandleEvent)
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	return events.HandleRepoStream(ctx, conn, scheduler, l.log)
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
		mapped, ok := l.labels[label.Val]
		if !ok {
			continue
		}
		u, err := syntax.ParseATURI(label.Uri)
		if err != nil {
			l.log.Warn("failed to parse label uri", "uri", label.Uri, "err", err)
			continue
		}
		did, err := u.Authority().AsDID()
		if err != nil {
			l.log.Warn("failed to parse label did", "uri", label.Uri, "err", err)
			continue
		}
		l.log.Debug("label", "did", did, "type", mapped, "uri", label.Uri)
	}
	return nil
}

func buildLabelMapping(policies *bsky.LabelerDefs_LabelerPolicies) map[string]string {
	m := make(map[string]string)
	for _, builtIn := range []string{
		LabelPorn,
		LabelSexual,
		LabelNudity,
		LabelGraphicMedia,
	} {
		m[builtIn] = builtIn
	}
	for _, policy := range policies.LabelValueDefinitions {
		if policy.AdultOnly != nil && *policy.AdultOnly {
			m[policy.Identifier] = LabelSexual
		} else if policy.Blurs != "none" {
			m[policy.Identifier] = LabelGraphicMedia
		}
	}
	return m
}
