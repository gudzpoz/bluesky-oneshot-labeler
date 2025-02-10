package listener

import (
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
)

type JetstreamListener struct {
	log *slog.Logger

	db     *database.Service
	client *client.Client

	persistQueue chan string
}

func NewJetStreamListener(logger *slog.Logger) (*JetstreamListener, error) {
	config := client.DefaultClientConfig()
	config.WantedCollections = []string{"app.bsky.feed.post"}
	config.WebsocketURL = "wss://jetstream2.us-west.bsky.network/subscribe"

	listener := &JetstreamListener{
		log: logger,
		db:  database.Instance(),
	}

	scheduler := parallel.NewScheduler(
		runtime.NumCPU(), // language classification can be CPU intensive
		"jetstream",
		logger.WithGroup("scheduler"),
		listener.HandleEvent,
	)
	c, err := client.NewClient(config, logger.WithGroup("client"), scheduler)
	if err != nil {
		return nil, err
	}
	listener.client = c

	return listener, nil
}

func (l *JetstreamListener) HandleEvent(ctx context.Context, event *models.Event) error {
	if event.Kind != "commit" || event.Commit == nil {
		return nil
	}
	commit := event.Commit
	if commit.Operation != "create" || commit.Collection != "app.bsky.feed.post" {
		return nil
	}

	var post bsky.FeedPost
	if err := json.Unmarshal(commit.Record, &post); err != nil {
		return err
	}
	if post.Reply != nil {
		return nil
	}

	// TODO: We just hard-code things here.
	if !l.ShouldKeepFeedItem(&post) {
		return nil
	}

	// uri := "at://" + event.Did + "/" + commit.Collection + "/" + commit.RKey
	compactUri := event.Did + "/" + commit.RKey
	l.persistQueue <- compactUri
	return nil
}

func (l *JetstreamListener) Persist(done chan bool) {
	count := 0
	last := time.Now()
	for uri := range l.persistQueue {
		err := l.db.InsertFeedItem(uri)
		if err != nil {
			l.log.Error("failed to insert feed item", "uri", uri, "err", err)
		}
		if count%100 == 0 {
			now := time.Now()
			if now.Sub(last) > 10*time.Minute {
				err := l.db.PruneFeedEntries(now.Add(-48 * time.Hour))
				if err != nil {
					l.log.Error("failed to prune feed entries", "err", err)
				} else {
					err := l.db.IncrementalVacuum()
					if err != nil {
						l.log.Error("failed to vacuum database", "err", err)
					}
				}
			}
		}
	}
	done <- true
}

func (l *JetstreamListener) Run(ctx context.Context) chan bool {
	l.persistQueue = make(chan string, runtime.NumCPU()*32)
	go func() {
		for {
			l.log.Info("connecting to jetstream in 1 second")
			select {
			case <-ctx.Done():
				l.log.Info("context done, listening stopped")
				close(l.persistQueue)
				return
			case <-time.After(1 * time.Second):
				ahead := time.Now().UTC().Add(-10 * time.Minute).UnixMicro()
				if err := l.client.ConnectAndRead(ctx, &ahead); err != nil {
					l.log.Error("jetstream error", "err", err)
				}
				l.log.Info("jetstream disconnected")
			}
		}
	}()
	done := make(chan bool)
	go l.Persist(done)
	return done
}
