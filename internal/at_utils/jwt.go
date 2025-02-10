package at_utils

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/bluesky-social/indigo/atproto/crypto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

var signatureAlg = jwa.SignatureAlgorithm("DIDKeyMultibase")

func init() {
	jws.RegisterVerifier(signatureAlg, keyProvider{})
}

type keyProvider struct{}

// jws.Verifier
func (k keyProvider) Verify(payload []byte, signature []byte, key interface{}) error {
	pubKey, ok := key.(crypto.PublicKey)
	if !ok {
		return fmt.Errorf("invalid key type")
	}
	return pubKey.HashAndVerifyLenient(payload, signature)
}

// jws.VerifierFactory
func (k keyProvider) Create() (jws.Verifier, error) {
	return k, nil
}

// jws.KeyProvider
func (k keyProvider) FetchKeys(
	ctx context.Context, sink jws.KeySink,
	sig *jws.Signature, msg *jws.Message,
) error {
	payload := make(map[string]any)
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		return err
	}

	issuer, ok := payload["iss"]
	if !ok {
		return fmt.Errorf("no issuer found")
	}
	issuerStr, ok := issuer.(string)
	if !ok {
		return fmt.Errorf("issuer is not a string")
	}
	did, err := syntax.ParseDID(issuerStr)
	if err != nil {
		return err
	}
	ident, err := IdentityDirectory.LookupDID(ctx, did)
	if err != nil {
		return err
	}

	atKey, ok := ident.Keys["atproto"]
	if !ok {
		return fmt.Errorf("no atproto key found")
	}
	pubKey, err := crypto.ParsePublicMultibase(atKey.PublicKeyMultibase)
	if err != nil {
		return err
	}

	sink.Key(signatureAlg, pubKey)
	return nil
}

func VerifyJwtToken(ctx context.Context, token string) (*identity.Identity, error) {
	jwtToken, err := jwt.ParseString(
		token,
		jwt.WithKeyProvider(keyProvider{}),
	)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(jwtToken.Audience(), UserDid.String()) {
		return nil, fmt.Errorf("invalid audience")
	}

	reporter, err := syntax.ParseDID(jwtToken.Issuer())
	if err != nil {
		return nil, err
	}
	ident, err := IdentityDirectory.LookupDID(ctx, reporter)
	if err != nil {
		return nil, err
	}
	return ident, nil
}
