package listener

import (
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"log/slog"
	"strings"
)

type AccountWatcher struct {
	db *database.Service

	offenderPostRatio float64

	notifier *BlockNotifier
}

func NewAccountWatcher(logger *slog.Logger) (*AccountWatcher, error) {
	db := database.Instance()

	offenderThreshold, err := db.GetConfigFloat("offender-post-ratio", 0.0)
	if err != nil {
		return nil, err
	}
	if offenderThreshold == 0.0 {
		offenderThreshold = config.OffenderThreshold
		if err := db.SetConfigFloat("offender-post-ratio", offenderThreshold); err != nil {
			return nil, err
		}
	}

	notifier, err := NewBlockNotifier(logger.WithGroup("notifier"))
	if err != nil {
		return nil, err
	}

	return &AccountWatcher{
		db:       db,
		notifier: notifier,

		offenderPostRatio: offenderThreshold,
	}, nil
}

func (w *AccountWatcher) Listen(ctx context.Context) {
	// TODO
}

func (w *AccountWatcher) CheckAccount(id int64, did string, count int64) {
	// TODO
	w.db.InsertBlock(id)
	w.notifier.Notify(&Block{
		Id:         id,
		CompactDid: strings.TrimPrefix(did, "did:"),
	})
}
