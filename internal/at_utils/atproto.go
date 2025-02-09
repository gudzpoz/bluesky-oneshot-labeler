package at_utils

import (
	"bluesky-oneshot-labeler/internal/config"
	"bluesky-oneshot-labeler/internal/database"
	"context"
	"encoding/base64"
	"net/http"
	"os"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/crypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/util/labels"
	"github.com/bluesky-social/indigo/xrpc"

	_ "github.com/bluesky-social/indigo/atproto/data"
	_ "github.com/bluesky-social/indigo/repo"
)

var AtProtoVersion int64 = 1
var IdentityDirectory = DefaultDirectory()
var Client *xrpc.Client
var UserDid syntax.DID
var KeyP256 *crypto.PrivateKeyP256

func DefaultDirectory() identity.Directory {
	dir := identity.DefaultDirectory()
	inner := dir.(*identity.CacheDirectory).Inner
	base := inner.(*identity.BaseDirectory)
	// workaround: "DID method not supported: "
	base.SkipDNSDomainSuffixes = []string{}
	base.HTTPClient.Transport = http.DefaultTransport
	return dir
}

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
	client.Client = util.RobustHTTPClient()
	Client = client
	UserDid = syntax.DID(client.Auth.Did)
	return nil
}

func InitKeys() error {
	db := database.Instance()
	keyStr, err := db.GetConfig("server-key", "")
	if err != nil {
		return err
	}
	if keyStr == "" {
		key, err := crypto.GeneratePrivateKeyP256()
		if err != nil {
			return err
		}
		keyStr = base64.StdEncoding.EncodeToString(key.Bytes())
		err = db.SetConfig("server-key", keyStr)
		if err != nil {
			return err
		}
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyStr)
	if err != nil {
		return err
	}

	key, err := crypto.ParsePrivateBytesP256(keyBytes)
	if err != nil {
		return err
	}
	KeyP256 = key
	return nil
}

func SignLabel(label *labels.UnsignedLabel) (*atproto.LabelDefs_Label, error) {
	bytes, err := label.BytesForSigning()
	if err != nil {
		return nil, err
	}
	sig, err := KeyP256.HashAndSign(bytes)
	if err != nil {
		return nil, err
	}
	return &atproto.LabelDefs_Label{
		Sig: sig,
		// Copy other fields. Hopefully the spec does not change too often?
		Cid: label.Cid,
		Cts: label.Cts,
		Exp: label.Exp,
		Neg: label.Neg,
		Src: label.Src,
		Uri: label.Uri,
		Val: label.Val,
		Ver: label.Ver,
	}, nil
}
