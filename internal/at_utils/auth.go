// From bluesky-social/indigo: cmd/goat/auth.go
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package at_utils

import (
	"bluesky-oneshot-labeler/internal/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
)

var ErrNoAuthSession = errors.New("no auth session found")

var sessionFile = config.SessionFile

type AuthSession struct {
	DID          syntax.DID `json:"did"`
	Password     string     `json:"password"`
	RefreshToken string     `json:"session_token"`
	PDS          string     `json:"pds"`
}

func persistAuthSession(sess *AuthSession) error {

	fPath := sessionFile

	f, err := os.OpenFile(fPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	authBytes, err := json.MarshalIndent(sess, "", "  ")
	if err != nil {
		return err
	}
	_, err = f.Write(authBytes)
	return err
}

func loadAuthClient(ctx context.Context) (*xrpc.Client, error) {

	// TODO: could also load from env var / cctx

	fPath := sessionFile

	fBytes, err := os.ReadFile(fPath)
	if err != nil {
		return nil, err
	}

	var sess AuthSession
	err = json.Unmarshal(fBytes, &sess)
	if err != nil {
		return nil, err
	}

	client := xrpc.Client{
		Host: sess.PDS,
		Auth: &xrpc.AuthInfo{
			Did: sess.DID.String(),
			// NOTE: using refresh in access location for "refreshSession" call
			AccessJwt:  sess.RefreshToken,
			RefreshJwt: sess.RefreshToken,
		},
	}
	resp, err := comatproto.ServerRefreshSession(ctx, &client)
	if err != nil {
		// TODO: if failure, try creating a new session from password (2fa tokens are only valid once, so not reused)
		fmt.Println("trying to refresh auth from password...")
		as, err := refreshAuthSession(ctx, sess.DID.AtIdentifier(), sess.Password, sess.PDS, "")
		if err != nil {
			return nil, err
		}
		client.Auth.AccessJwt = as.RefreshToken
		client.Auth.RefreshJwt = as.RefreshToken
		resp, err = comatproto.ServerRefreshSession(ctx, &client)
		if err != nil {
			return nil, err
		}
	}
	client.Auth.AccessJwt = resp.AccessJwt
	client.Auth.RefreshJwt = resp.RefreshJwt

	return &client, nil
}

func refreshAuthSession(ctx context.Context, username syntax.AtIdentifier, password, pdsURL, authFactorToken string) (*AuthSession, error) {

	var did syntax.DID
	if pdsURL == "" {
		dir := identity.DefaultDirectory()
		ident, err := dir.Lookup(ctx, username)
		if err != nil {
			return nil, err
		}

		pdsURL = ident.PDSEndpoint()
		if pdsURL == "" {
			return nil, fmt.Errorf("empty PDS URL")
		}
		did = ident.DID
	}

	if did == "" && username.IsDID() {
		did, _ = username.AsDID()
	}

	client := xrpc.Client{
		Host: pdsURL,
	}
	var token *string
	if authFactorToken != "" {
		token = &authFactorToken
	}
	sess, err := comatproto.ServerCreateSession(ctx, &client, &comatproto.ServerCreateSession_Input{
		Identifier:      username.String(),
		Password:        password,
		AuthFactorToken: token,
	})
	if err != nil {
		return nil, err
	}

	// TODO: check account status?
	// TODO: warn if email isn't verified?
	// TODO: check that sess.Did matches username
	if did == "" {
		did, err = syntax.ParseDID(sess.Did)
		if err != nil {
			return nil, err
		}
	} else if sess.Did != did.String() {
		return nil, fmt.Errorf("session DID didn't match expected: %s != %s", sess.Did, did)
	}

	authSession := AuthSession{
		DID:          did,
		Password:     password,
		PDS:          pdsURL,
		RefreshToken: sess.RefreshJwt,
	}
	if err = persistAuthSession(&authSession); err != nil {
		return nil, err
	}
	return &authSession, nil
}
