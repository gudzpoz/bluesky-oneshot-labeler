package listener

import (
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"slices"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/parallel"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/pemistahl/lingua-go"
)

type JetstreamListener struct {
	log *slog.Logger

	db       *database.Service
	client   *client.Client
	linguist lingua.LanguageDetector

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

	listener.linguist = lingua.NewLanguageDetectorBuilder().
		FromLanguages(
			lingua.Chinese,
			lingua.Japanese,
			lingua.Korean,
			lingua.English,
		).
		WithLowAccuracyMode().
		WithPreloadedLanguageModels().
		Build()

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
	if !slices.Contains(post.Langs, "zh") {
		return nil
	}

	text := post.Text
	if text == "" {
		if post.Embed != nil {
			if post.Embed.EmbedExternal != nil {
				text = post.Embed.EmbedExternal.External.Description
				if text == "" {
					text = post.Embed.EmbedExternal.External.Title
				}
			}
			if post.Embed.EmbedImages != nil {
				for _, image := range post.Embed.EmbedImages.Images {
					text += image.Alt
				}
			}
			if post.Embed.EmbedVideo != nil {
				if post.Embed.EmbedVideo.Alt != nil {
					text = *post.Embed.EmbedVideo.Alt
				}
			}
		}
	}
	if text == "" {
		return nil
	}
	if !l.hasChinese(text) {
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

// https://github.com/pemistahl/lingua-go/issues/38
func (l *JetstreamListener) hasChinese(text string) bool {
	langs := l.linguist.DetectMultipleLanguagesOf(text)

	hasChinese := false
	hasJapanese := false
	for _, lang := range langs {
		if lang.Language() == lingua.Chinese {
			hasChinese = true
		} else if lang.Language() == lingua.Japanese {
			hasJapanese = true
		}
	}
	if !hasChinese && hasJapanese {
		zhCount := 0
		jaCount := 0
		for _, char := range text {
			if isJapanese(char) {
				jaCount++
			}
			if isChinese(char) {
				zhCount++
			}
		}
		ratio := float64(zhCount) / float64(jaCount)
		if ratio > 1.5 {
			hasChinese = true
		}
	}

	return hasChinese
}

func isChinese(c rune) bool {
	// Chinese Unicode range
	if (c >= '\u3400' && c <= '\u4db5') || // CJK Unified Ideographs Extension A
		(c >= '\u4e00' && c <= '\u9fed') || // CJK Unified Ideographs
		(c >= '\uf900' && c <= '\ufaff') { // CJK Compatibility Ideographs
		return true
	}

	return false
}

func isJapanese(c rune) bool {
	// Japanese Unicode range
	if (c >= '\u3021' && c <= '\u3029') || // Japanese Hanzi
		(c >= '\u3040' && c <= '\u309f') || // Hiragana
		(c >= '\u30a0' && c <= '\u30ff') || // Katakana
		(c >= '\u31f0' && c <= '\u31ff') || // Katakana Phonetic Extension
		(c >= '\uf900' && c <= '\ufaff') { // CJK Compatibility Ideographs
		return true
	}

	return false
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
