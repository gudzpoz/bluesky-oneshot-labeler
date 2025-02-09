package at_utils

import (
	"bluesky-oneshot-labeler/internal/config"
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/data"
	lex_util "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/xrpc"
)

const (
	LabelPornString         = "porn"
	LabelSexualString       = "sexual"
	LabelNudityString       = "nudity"
	LabelGraphicMediaString = "graphic-media"
	LabelOffenderString     = "offender"
	LabelOthersString       = "others"
)

func requestPlcToken() (string, error) {
	err := atproto.IdentityRequestPlcOperationSignature(context.Background(), Client)
	if err != nil {
		return "", err
	}

	r := bufio.NewReader(os.Stdin)
	fmt.Fprint(
		os.Stderr,
		"Bluesky should have sent you a token to publish the labeler.\n"+
			"Please paste it here: ",
	)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

type mapWrapper struct {
	inner map[string]any
}

func (m *mapWrapper) MarshalCBOR(w io.Writer) error {
	bytes, err := data.MarshalCBOR(m.inner)
	if err != nil {
		return err
	}
	_, err = w.Write(bytes)
	return err
}

func PublishLabelerInfo(ctx context.Context) error {
	// // Unfortunately, this API does not yet support the returned credentials and throws parsing errors.
	// credentials, err := atproto.IdentityGetRecommendedDidCredentials(context.Background(), Client)

	// We have to manually construct the request.
	credentials := make(map[string]any)
	credentialApi := "com.atproto.identity.getRecommendedDidCredentials"
	if err := Client.Do(ctx, xrpc.Query, "", credentialApi, nil, nil, &credentials); err != nil {
		return err
	}

	pubKey, err := KeyP256.PublicKey()
	if err != nil {
		return err
	}
	pubKeyStr := pubKey.DIDKey()

	var alsoKnownAs []string
	if value, ok := credentials["alsoKnownAs"]; ok {
		if alsoKnownAs, ok = value.([]string); !ok {
			return fmt.Errorf("alsoKnownAs is not a string array: %T", value)
		}
	}
	var rotationKeys []string
	if value, ok := credentials["rotationKeys"]; ok {
		if rotationKeys, ok = value.([]string); !ok {
			return fmt.Errorf("rotationKeys is not a string array: %T", value)
		}
	}
	services := make(map[string]any)
	if value, ok := credentials["services"]; ok {
		if services, ok = value.(map[string]any); !ok {
			return fmt.Errorf("services is not a map: %T", value)
		}
	}
	verificationMethods := make(map[string]any)
	if value, ok := credentials["verificationMethods"]; ok {
		if verificationMethods, ok = value.(map[string]any); !ok {
			return fmt.Errorf("verificationMethods is not a map: %T", value)
		}
	}

	if original, ok := verificationMethods["atproto_label"]; ok {
		originalStr, ok := original.(string)
		if !ok || originalStr != pubKeyStr {
			return fmt.Errorf("verificationMethods[atproto_label] already exists: %s", original)
		}
		if originalStr == pubKeyStr {
			if labeler, ok := services["atproto_labeler"]; ok {
				if labelerMap, ok := labeler.(map[string]any); ok {
					labelerType, ok1 := labelerMap["type"]
					labelerEndpoint, ok2 := labelerMap["endpoint"]
					if ok1 && labelerType == "AtprotoLabeler" &&
						ok2 && labelerEndpoint == "https://"+config.Host {
						slog.Info("Labeler info already published")
						return nil
					}
				}
			}
		}
	}
	verificationMethods["atproto_label"] = pubKeyStr
	services["atproto_labeler"] = map[string]any{
		"type":     "AtprotoLabeler",
		"endpoint": "https://" + config.Host,
	}

	plcToken, err := requestPlcToken()
	if err != nil {
		return err
	}

	input := atproto.IdentitySignPlcOperation_Input{
		Token:        &plcToken,
		AlsoKnownAs:  alsoKnownAs,
		RotationKeys: rotationKeys,
		Services: &lex_util.LexiconTypeDecoder{
			Val: &mapWrapper{inner: services},
		},
		VerificationMethods: &lex_util.LexiconTypeDecoder{
			Val: &mapWrapper{inner: verificationMethods},
		},
	}

	// Again, we have to manually construct the request.
	signed := make(map[string]any)
	signApi := "com.atproto.identity.signPlcOperation"
	if err := Client.Do(ctx, xrpc.Procedure, "application/json", signApi, nil, &input, &signed); err != nil {
		return err
	}
	submitApi := "com.atproto.identity.submitPlcOperation"
	if err := Client.Do(ctx, xrpc.Procedure, "application/json", submitApi, nil, signed, nil); err != nil {
		return err
	}
	return atproto.IdentityUpdateHandle(ctx, Client, &atproto.IdentityUpdateHandle_Input{
		Handle: config.Username,
	})
}

func PublishLabelInfo(ctx context.Context) error {
	labels := []string{
		LabelPornString,
		LabelSexualString,
		LabelNudityString,
		LabelGraphicMediaString,
		LabelOffenderString,
		LabelOthersString,
	}
	labelPointers := make([]*string, len(labels))
	for i, label := range labels {
		labelPointers[i] = &label
	}
	trueValue := true

	service := bsky.LabelerService{
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Policies: &bsky.LabelerDefs_LabelerPolicies{
			LabelValueDefinitions: []*atproto.LabelDefs_LabelValueDefinition{
				{
					Identifier: LabelOffenderString,
					AdultOnly:  &trueValue,
					Blurs:      "content",
					Severity:   "alert",
					Locales: []*atproto.LabelDefs_LabelValueDefinitionStrings{
						{
							Name:        "Incorrigible",
							Lang:        "en",
							Description: "Users who seldom labels their not-suitable-for-whatever contents.",
						},
					},
				},
				{
					Identifier: LabelOthersString,
					Severity:   "inform",
					Locales: []*atproto.LabelDefs_LabelValueDefinitionStrings{
						{
							Name:        "Others",
							Lang:        "en",
							Description: "Users who has posted contents flagged by the upstream moderation servichas posted contents flagged by the upstream moderation service.",
						},
					},
				},
			},
			LabelValues: labelPointers,
		},
	}

	exists, err := labelInfoExists(ctx)
	if err != nil {
		return err
	}
	if exists {
		_, err = atproto.RepoPutRecord(ctx, Client, &atproto.RepoPutRecord_Input{
			Collection: "app.bsky.labeler.service",
			Rkey:       "self",
			Repo:       UserDid.String(),
			Record: &lex_util.LexiconTypeDecoder{
				Val: &service,
			},
			Validate: &trueValue,
		})
	} else {
		self := "self"
		_, err = atproto.RepoCreateRecord(ctx, Client, &atproto.RepoCreateRecord_Input{
			Collection: "app.bsky.labeler.service",
			Rkey:       &self,
			Repo:       UserDid.String(),
			Record: &lex_util.LexiconTypeDecoder{
				Val: &service,
			},
			Validate: &trueValue,
		})
	}

	return err
}

func IsRecordNotFound(err error) bool {
	if inner, ok := err.(*xrpc.Error); ok {
		if xrpcErr, ok := inner.Wrapped.(*xrpc.XRPCError); ok && xrpcErr.ErrStr == "RecordNotFound" {
			return true
		}
	}
	return false
}

func labelInfoExists(ctx context.Context) (bool, error) {
	params := map[string]interface{}{
		"repo":       UserDid.String(),
		"collection": "app.bsky.labeler.service",
		"rkey":       "self",
	}
	recordApi := "com.atproto.repo.getRecord"
	labelerDetails := bsky.LabelerDefs_LabelerViewDetailed{}
	if err := Client.Do(ctx, xrpc.Query, "", recordApi, params, nil, &labelerDetails); err != nil {
		if !IsRecordNotFound(err) {
			return false, err
		}
		return false, nil
	}

	return true, nil
}
