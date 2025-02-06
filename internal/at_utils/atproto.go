package at_utils

import (
	"bluesky-oneshot-labeler/internal/config"
	"context"
	"os"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

var IdentityDirectory = identity.DefaultDirectory()
var Client *xrpc.Client

func IsRegularFile(path string) bool {
	if stat, err := os.Stat(path); err == nil && !stat.IsDir() {
		return true
	}
	return false
}

func InitXrpcClient(ctx context.Context) error {
	if !IsRegularFile(sessionFile) {
		ident, err := syntax.ParseAtIdentifier(config.Username)
		if err != nil {
			return err
		}
		session, err := refreshAuthSession(ctx, *ident, config.Password, "", "")
		if err != nil {
			return err
		}

		err = persistAuthSession(session)
		if err != nil {
			return err
		}
	}

	client, err := loadAuthClient(ctx)
	if err != nil {
		return err
	}
	Client = client
	return nil
}
