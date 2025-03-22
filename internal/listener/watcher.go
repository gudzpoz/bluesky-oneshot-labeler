package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"golang.org/x/time/rate"
)

type upstreamLabel struct {
	Uid   int64
	Did   string
	Count int64
}

type AccountWatcher struct {
	db  *database.Service
	log *slog.Logger

	queue    chan *upstreamLabel
	limiter  *rate.Limiter
	notifier *BlockNotifier

	offendingPostRatio float64
}

func NewAccountWatcher(logger *slog.Logger) (*AccountWatcher, error) {
	db := database.Instance()

	ratio := config.OffendingPostRatio
	if ratio == 0 || ratio > 1 {
		return nil, fmt.Errorf("invalid offending post ratio: %f", ratio)
	}

	notifier, err := NewBlockNotifier(logger.WithGroup("notifier"))
	if err != nil {
		return nil, err
	}

	w := &AccountWatcher{
		db:       db,
		log:      logger.WithGroup("watcher"),
		queue:    make(chan *upstreamLabel, 4096),
		limiter:  rate.NewLimiter(rate.Limit(config.AppViewRateLimit), config.AppViewRateLimit*2),
		notifier: notifier,

		offendingPostRatio: ratio,
	}
	return w, nil
}

func (w *AccountWatcher) Listen(ctx context.Context, done chan bool) {
	batch := make(map[string]*upstreamLabel, 25)
	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				w.checkBatch(ctx, batch)
			}
			done <- true
			return

		case label := <-w.queue:
			compact := strings.TrimPrefix(label.Did, "did:")
			blocked, err := w.db.IsUserBlocked(compact)
			if err != nil {
				w.log.Error("failed to check if user is blocked", "err", err)
				continue
			}
			if blocked {
				continue
			}

			batch[label.Did] = label
			if len(batch) >= 25 {
				w.checkBatch(ctx, batch)
				batch = make(map[string]*upstreamLabel, 25)
			}
		case <-time.After(time.Second * 5):
			if len(batch) > 0 {
				w.checkBatch(ctx, batch)
				batch = make(map[string]*upstreamLabel, 25)
			}
		}
	}
}

func (w *AccountWatcher) checkBatch(ctx context.Context, labels map[string]*upstreamLabel) {
	if err := w.limiter.Wait(ctx); err != nil {
		w.log.Error("failed to wait for rate limiter", "err", err)
		return
	}

	done := make(chan bool)
	go func() {
		w.checkBatchWorker(ctx, labels)
		done <- true
	}()
	<-done
}

func (w *AccountWatcher) checkBatchWorker(ctx context.Context, labels map[string]*upstreamLabel) {
	actors := make([]string, 0, len(labels))
	for _, label := range labels {
		actors = append(actors, label.Did)
	}
	profiles, err := bsky.ActorGetProfiles(ctx, at_utils.PubClient, actors)
	if err != nil {
		w.log.Error("failed to get profiles", "err", err)
		for _, label := range labels {
			select {
			case w.queue <- label:
			default:
				w.log.Error("failed to requeue label", "did", label.Did)
			}
		}
		return
	}

	candidates := make([]*upstreamLabel, 0, len(labels))
	for _, profile := range profiles.Profiles {
		did := profile.Did
		posts := profile.PostsCount
		label, ok := labels[did]
		if !ok || posts == nil {
			continue
		}
		offendingPostUpperLimit := int64(float64(*posts) * w.offendingPostRatio)
		if label.Count > offendingPostUpperLimit {
			candidates = append(candidates, label)
			continue
		}
		count, err := w.db.TotalCounts(label.Uid)
		if err != nil {
			w.log.Error("failed to get total counts", "err", err)
			continue
		}
		if count > offendingPostUpperLimit {
			candidates = append(candidates, label)
		}
	}

	for _, label := range candidates {
		blockId, err := w.db.InsertBlock(label.Uid)
		if err != nil {
			w.log.Error("failed to insert block", "err", err)
			continue
		}
		w.notifier.Notify(&Block{
			Id:         blockId,
			CompactDid: strings.TrimPrefix(label.Did, "did:"),
		})
	}
}

func (w *AccountWatcher) CheckAccount(uid int64, did string, count int64) {
	w.queue <- &upstreamLabel{
		Uid:   uid,
		Did:   did,
		Count: count,
	}
}
