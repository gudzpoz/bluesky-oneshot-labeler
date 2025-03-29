package listener

import (
	"bluesky-oneshot-labeler/internal/at_utils"
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
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
)

type SerializableInt64 atomic.Int64

type FeedStats struct {
	StartedAt time.Time

	ItemsReceived        SerializableInt64
	ItemsPersisted       SerializableInt64
	ItemsBlockedByDb     SerializableInt64
	ItemsBlockedByCsv    SerializableInt64
	ItemsBlockedByFilter SerializableInt64
}

func (i *SerializableInt64) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.Load())
}
func (i *SerializableInt64) Inc() {
	(*atomic.Int64)(i).Add(1)
}
func (i *SerializableInt64) Load() int64 {
	return (*atomic.Int64)(i).Load()
}

type JetstreamListener struct {
	log *slog.Logger

	db       *database.Service
	client   *client.Client
	notifier *BlockNotifier

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
	blockCount, err := db.LastBlockId()
	if err != nil {
		return nil, err
	}

	cursorUs, err := db.GetConfigInt("sync-time", time.Now().UTC().Add(-1*time.Minute).UnixMicro())
	if err != nil {
		return nil, err
	}
	syncTime.Store(cursorUs)

	listener := &JetstreamListener{
		log:         logger,
		db:          db,
		notifier:    upstream.Notifier(),
		bloomApprox: blockCount,
		bloomFilter: bloom.NewWithEstimates(uint(blockCount), 0.01),
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
	l.Stats.ItemsReceived.Inc()
	if event.Kind != "commit" || event.Commit == nil {
		return nil
	}
	commit := event.Commit
	if commit.Operation != "create" || commit.Collection != "app.bsky.feed.post" {
		return nil
	}
	at_utils.StoreLarger(&syncTime, event.TimeUS)

	var post bsky.FeedPost
	if err := json.Unmarshal(commit.Record, &post); err != nil {
		return err
	}
	if post.Reply != nil {
		return nil
	}

	if !l.ShouldKeepFeedItem(&post, event) {
		l.Stats.ItemsBlockedByFilter.Inc()
		return nil
	}

	did := event.Did
	compactDid := strings.TrimPrefix(did, "did:")
	if l.blockList.Contains(compactDid) {
		return nil
	}
	blockList := l.InBlockList(compactDid)
	if blockList != OutOfBlockList {
		l.incStats(blockList)
		return nil
	}
	if post.Embed != nil {
		record := post.Embed.EmbedRecord
		if record == nil && post.Embed.EmbedRecordWithMedia != nil {
			record = post.Embed.EmbedRecordWithMedia.Record
		}
		if record != nil {
			uri, err := syntax.ParseATURI(record.Record.Uri)
			if err == nil {
				embedDid := strings.TrimPrefix(uri.Authority().String(), "did:")
				blockList = l.InBlockList(embedDid)
				if blockList != OutOfBlockList {
					l.incStats(blockList)
					return nil
				}
			}
		}
	}

	if !l.ShouldKeepFeedItemCostly(ctx, &post, did) {
		l.Stats.ItemsBlockedByFilter.Inc()
		return nil
	}

	// uri := "at://" + event.Did + "/" + commit.Collection + "/" + commit.RKey
	compactUri := event.Did + "/" + commit.RKey
	l.log.Debug("keeping feed item", "uri", compactUri, "lang", post.Langs, "content", post.Text)
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
				l.Stats.ItemsPersisted.Inc()
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
				ahead := syncTime.Load() // syncTime initialized in the constructor
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
			err := l.notifier.ForAllLabelsSince(ctx, 0, func(block *Block, new bool) error {
				id := block.Id
				did := block.CompactDid
				if id > approx*2 {
					return RebuildFilterError{NewSize: id}
				}
				filter.AddString(did)
				if new {
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
	if l.blockList.Contains(did) {
		return BlockListCsv
	}

	if !l.bloomFilter.TestString(did) {
		return OutOfBlockList
	}
	labeled, err := l.db.IsUserBlocked(did)
	if err != nil {
		l.log.Error("failed to check if user is labeled", "err", err)
		return OutOfBlockList
	}
	if labeled {
		return BlockListDb
	}
	return OutOfBlockList
}

func (l *JetstreamListener) incStats(inBlockList int) {
	switch inBlockList {
	case BlockListDb:
		l.Stats.ItemsBlockedByDb.Inc()
	case BlockListCsv:
		l.Stats.ItemsBlockedByCsv.Inc()
	}
}
