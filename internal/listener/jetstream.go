package listener

import (
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
)

type FeedStats struct {
	StartedAt time.Time

	ItemsReceived        atomic.Int64
	ItemsPersisted       atomic.Int64
	ItemsBlockedByDb     atomic.Int64
	ItemsBlockedByCsv    atomic.Int64
	ItemsBlockedByFilter atomic.Int64
}

type JetstreamListener struct {
	log *slog.Logger

	db       *database.Service
	client   *client.Client
	notifier *LabelNotifier

	bloomApprox  int64
	bloomFilter  *bloom.BloomFilter
	blockList    *BlockListInSync
	listUpdated  chan bool
	persistQueue chan string

	Stats FeedStats
}

func NewJetStreamListener(upstream *LabelListener, blockList *BlockListInSync, logger *slog.Logger) (*JetstreamListener, error) {
	config := client.DefaultClientConfig()
	config.WantedCollections = []string{"app.bsky.feed.post"}
	config.WebsocketURL = "wss://jetstream2.us-west.bsky.network/subscribe"

	db := database.Instance()
	latest, err := db.LatestLabelId()
	if err != nil {
		return nil, err
	}

	listener := &JetstreamListener{
		log:         logger,
		db:          db,
		notifier:    upstream.Notifier(),
		bloomApprox: latest,
		bloomFilter: bloom.NewWithEstimates(uint(latest), 0.01),
		blockList:   blockList,
		listUpdated: make(chan bool, 1),

		persistQueue: make(chan string, runtime.NumCPU()*32),

		Stats: FeedStats{
			StartedAt: time.Now().UTC(),
		},
	}
	blockList.SetNotifier(listener.notifyListUpdated)

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

func (l *JetstreamListener) notifyListUpdated() {
	select {
	case l.listUpdated <- true:
	default:
	}
}

func (l *JetstreamListener) HandleEvent(ctx context.Context, event *models.Event) error {
	l.Stats.ItemsReceived.Add(1)
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

	if !l.ShouldKeepFeedItem(&post) {
		l.Stats.ItemsBlockedByFilter.Add(1)
		return nil
	}

	compactDid := strings.TrimPrefix(event.Did, "did:")
	if l.blockList.Contains(compactDid) {
		return nil
	}
	blockList := l.InBlockList(compactDid)
	if blockList != OutOfBlockList {
		switch blockList {
		case BlockListDb:
			l.Stats.ItemsBlockedByDb.Add(1)
		case BlockListCsv:
			l.Stats.ItemsBlockedByCsv.Add(1)
		}
		return nil
	}

	// uri := "at://" + event.Did + "/" + commit.Collection + "/" + commit.RKey
	compactUri := event.Did + "/" + commit.RKey
	l.persistQueue <- compactUri
	return nil
}

func (l *JetstreamListener) Persist(ctx context.Context, done chan bool) {
	lock := sync.Mutex{}

	go func() {
		count := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.listUpdated:
				count++
				if count%32 == 0 {
					err := l.PruneBlockedEntries(&lock)
					if err != nil {
						l.log.Error("failed to prune blocked entries", "err", err)
					}
				}
			}
		}
	}()

	count := 0
	last := time.Now()
loop:
	for {
		select {
		case uri := <-l.persistQueue:
			lock.Lock()
			err := l.db.InsertFeedItem(uri)
			lock.Unlock()
			if err == nil {
				l.Stats.ItemsPersisted.Add(1)
			} else {
				l.log.Error("failed to insert feed item", "uri", uri, "err", err)
			}
			if count%100 == 0 {
				now := time.Now()
				if now.Sub(last) > 10*time.Minute {
					lock.Lock()
					err := l.db.PruneFeedEntries(now.Add(-48 * time.Hour))
					if err != nil {
						l.log.Error("failed to prune feed entries", "err", err)
					} else {
						err := l.db.IncrementalVacuum()
						if err != nil {
							l.log.Error("failed to vacuum database", "err", err)
						}
					}
					lock.Unlock()
				}
			}
		case <-ctx.Done():
			break loop
		}
	}
	l.log.Info("persist context done")
	done <- true
}

func (l *JetstreamListener) PruneBlockedEntries(lock *sync.Mutex) error {
	l.log.Debug("pruning blocked entries")
	return l.db.PruneEntries(func(compactUri string) bool {
		i := strings.Index(compactUri, "/")
		if i == -1 {
			return false
		}
		compactDid := strings.TrimPrefix(compactUri[:i], "did:")
		return l.InBlockList(compactDid) != OutOfBlockList
	}, lock)
}

func (l *JetstreamListener) Run(ctx context.Context) chan bool {
	persitCtx, cancelPersist := context.WithCancel(context.Background())
	go l.KeepBloomFilterInSync(ctx)

	go func() {
		for {
			l.log.Debug("connecting to jetstream in 1 second")
			select {
			case <-ctx.Done():
				l.client.Scheduler.Shutdown()
				l.log.Info("context done, jetstream stopped, now stopping persist")
				cancelPersist()
				return
			case <-time.After(1 * time.Second):
				// TODO: This results in duplicate entries on reconnect/restart.
				ahead := time.Now().UTC().Add(-1 * time.Minute).UnixMicro()
				if err := l.client.ConnectAndRead(ctx, &ahead); err != nil {
					l.log.Error("jetstream error", "err", err)
				}
				l.log.Debug("jetstream disconnected")
			}
		}
	}()
	done := make(chan bool)
	go l.Persist(persitCtx, done)
	return done
}

type RebuildFilterError struct {
	NewSize int64
}

func (e RebuildFilterError) Error() string {
	return fmt.Sprintf("rebuild filter to size %d", e.NewSize)
}

func (l *JetstreamListener) KeepBloomFilterInSync(ctx context.Context) {
	approx := l.bloomApprox
	filter := l.bloomFilter
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := l.notifier.ForAllLabelsSince(ctx, 0, func(label *database.Label, xe *events.XRPCStreamEvent) error {
				var id int64
				var did string
				if label != nil {
					id = label.Id
					did = label.Did
				} else {
					id = xe.LabelLabels.Seq
					did = strings.TrimPrefix(xe.LabelLabels.Labels[0].Uri, "at://did:")
				}
				if id > approx*2 {
					return RebuildFilterError{NewSize: id}
				}
				filter.AddString(did)
				if label == nil {
					l.log.Debug("adding to db filter", "did", did)
					l.notifyListUpdated()
				}
				return nil
			})
			if err != nil {
				if newSize, ok := err.(RebuildFilterError); ok {
					l.log.Debug("rebuilding bloom filter", "new_size", newSize.NewSize)
					filter = bloom.NewWithEstimates(uint(newSize.NewSize), 0.01)
					approx = newSize.NewSize
					l.bloomApprox = approx
					l.bloomFilter = filter
				} else {
					l.log.Error("bloom filter sync error", "err", err)
				}
			}
		}
	}
}

const (
	OutOfBlockList = 0
	BlockListDb    = 1
	BlockListCsv   = 2
)

func (l *JetstreamListener) InBlockList(did string) int {
	l.log.Debug("checking if in block list", "did", did)
	if l.blockList.Contains(did) {
		return BlockListCsv
	}

	if !l.bloomFilter.TestString(did) {
		return OutOfBlockList
	}
	labeled, err := l.db.IsUserLabeled(did)
	if err != nil {
		l.log.Error("failed to check if user is labeled", "err", err)
		return OutOfBlockList
	}
	if labeled {
		return BlockListDb
	}
	return OutOfBlockList
}
