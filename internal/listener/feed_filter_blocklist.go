package listener

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/fsnotify/fsnotify"
)

type BlockListInSync struct {
	filter *bloom.BloomFilter
	list   map[string]struct{}

	log      *slog.Logger
	csvPath  string
	watcher  *fsnotify.Watcher
	notifier func()
}

func NewBlockListInSync(csvPath string, logger *slog.Logger) (*BlockListInSync, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &BlockListInSync{
		filter:   bloom.NewWithEstimates(100, 0.01),
		list:     make(map[string]struct{}),
		log:      logger,
		csvPath:  csvPath,
		watcher:  watcher,
		notifier: func() {},
	}, nil
}

func (b *BlockListInSync) SetNotifier(notifier func()) {
	b.notifier = notifier
}

func (b *BlockListInSync) Contains(did string) bool {
	// b.filter is CoW, so we don't need to lock it.
	if !b.filter.TestString(did) {
		return false
	}
	_, ok := b.list[did]
	return ok
}

func (b *BlockListInSync) update() error {
	reader, err := os.Open(b.csvPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	var count uint
	list := make(map[string]struct{})

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		i := strings.Index(line, ",")
		var did string
		if i == -1 {
			did = line
		} else {
			did = line[:i]
		}
		did = strings.TrimSpace(did)
		did = strings.Trim(did, `"`)
		if !strings.HasPrefix(did, "did:") {
			continue
		}
		did = strings.TrimPrefix(did, "did:")
		list[did] = struct{}{}
		count++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	filter := bloom.NewWithEstimates(count, 0.01)
	for did := range list {
		filter.AddString(did)
	}

	b.filter = filter
	b.list = list
	b.log.Info("blocklist updated", "count", count)
	b.notifier()
	return nil
}

func (b *BlockListInSync) Run(ctx context.Context) chan bool {
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan bool)
	go func() {
		defer b.Close(done)

		if b.csvPath == "" {
			<-ctx.Done()
			return
		}

		err := b.update()
		if err != nil {
			b.log.Error("failed to update blocklist", "err", err)
			cancel(err)
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-b.watcher.Events:
				if !ok {
					b.log.Error("watcher closed")
					cancel(context.Canceled)
					return
				}
				if event.Has(fsnotify.Write) {
					err := b.update()
					if err != nil {
						b.log.Error("failed to update blocklist", "err", err)
					}
				}
			}
		}
	}()
	err := b.watcher.Add(b.csvPath)
	if err != nil {
		b.log.Error("failed to watch blocklist", "err", err)
		cancel(err)
	}
	return done
}

func (b *BlockListInSync) Close(done chan bool) {
	b.log.Info("blocklist sync stopped")
	done <- true
	if err := b.watcher.Close(); err != nil {
		b.log.Error("failed to close watcher", "err", err)
	}
}
