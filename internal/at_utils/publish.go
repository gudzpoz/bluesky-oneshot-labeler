package at_utils

import (
	"bluesky-oneshot-labeler/internal/config"
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
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

func requestPlcToken(ctx context.Context) (string, error) {
	token := config.PlcToken
	if token != "" {
		return token, nil
	}

	err := atproto.IdentityRequestPlcOperationSignature(ctx, Client)
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

	ident, err := BaseDirectory.ResolveDID(ctx, UserDid)
	if err != nil {
		return err
	}

	keyExists := false
	for _, vm := range ident.VerificationMethod {
		fmt.Println(vm.PublicKeyMultibase)
		fmt.Println(pubKeyStr)
		if vm.PublicKeyMultibase == strings.TrimPrefix(pubKeyStr, "did:key:") {
			keyExists = true
			break
		}
		splits := strings.Split(vm.ID, "#")
		if len(splits) == 2 && splits[1] == "atproto_label" {
			return fmt.Errorf("verificationMethods[atproto_label] already exists: %s", vm.PublicKeyMultibase)
		}
	}
	serviceExists := 0
	for _, service := range ident.Service {
		if service.Type == "AtprotoLabeler" && service.ServiceEndpoint == "https://"+config.Host {
			serviceExists |= 0b01
		}
		if service.Type == "BskyFeedGenerator" && service.ServiceEndpoint == "https://"+config.Host {
			serviceExists |= 0b10
		}
	}
	if keyExists && serviceExists == 0b11 {
		slog.Info("Labeler info already published")
		return nil
	}

	var alsoKnownAs []any
	if value, ok := credentials["alsoKnownAs"]; ok {
		if alsoKnownAs, ok = value.([]any); !ok {
			return fmt.Errorf("alsoKnownAs is not a string array: %T", value)
		}
	}
	var rotationKeys []any
	if value, ok := credentials["rotationKeys"]; ok {
		if rotationKeys, ok = value.([]any); !ok {
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

	verificationMethods["atproto_label"] = pubKeyStr
	services["atproto_labeler"] = map[string]any{
		"type":     "AtprotoLabeler",
		"endpoint": "https://" + config.Host,
	}
	services["bsky_fg"] = map[string]any{
		"type":     "BskyFeedGenerator",
		"endpoint": "https://" + config.Host,
	}

	plcToken, err := requestPlcToken(ctx)
	if err != nil {
		return err
	}

	// Again, we have to manually construct the request.
	input := map[string]any{
		"token":               plcToken,
		"alsoKnownAs":         alsoKnownAs,
		"rotationKeys":        rotationKeys,
		"services":            services,
		"verificationMethods": verificationMethods,
	}
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

	hide := "hide"
	warn := "warn"
	service := bsky.LabelerService{
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Policies: &bsky.LabelerDefs_LabelerPolicies{
			LabelValueDefinitions: []*atproto.LabelDefs_LabelValueDefinition{
				{
					Identifier:     LabelOffenderString,
					AdultOnly:      &trueValue,
					Blurs:          "content",
					DefaultSetting: &hide,
					Severity:       "alert",
					Locales: []*atproto.LabelDefs_LabelValueDefinitionStrings{
						{
							Name:        "Incorrigible",
							Lang:        "en",
							Description: "Users who seldom labels their not-suitable-for-whatever contents.",
						},
					},
				},
				{
					Identifier:     LabelOthersString,
					Blurs:          "none",
					DefaultSetting: &warn,
					Severity:       "inform",
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

	prevRecord, err := labelInfoExists(ctx)
	if err != nil {
		return err
	}
	if prevRecord == "" {
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
	} else {
		_, err = atproto.RepoPutRecord(ctx, Client, &atproto.RepoPutRecord_Input{
			SwapRecord: &prevRecord,
			Collection: "app.bsky.labeler.service",
			Rkey:       "self",
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

func labelInfoExists(ctx context.Context) (string, error) {
	params := map[string]interface{}{
		"repo":       UserDid.String(),
		"collection": "app.bsky.labeler.service",
		"rkey":       "self",
	}
	recordApi := "com.atproto.repo.getRecord"
	labelerDetails := bsky.LabelerDefs_LabelerViewDetailed{}
	if err := Client.Do(ctx, xrpc.Query, "", recordApi, params, nil, &labelerDetails); err != nil {
		if !IsRecordNotFound(err) {
			return "", err
		}
		return "", nil
	}

	return labelerDetails.Cid, nil
}

func PublishFeedInfo(ctx context.Context) error {
	avatar := config.FeedAvatar
	if avatar == "" {
		return fmt.Errorf("FEED_AVATAR is not set")
	}

	var encoding string
	if strings.HasSuffix(avatar, ".png") {
		encoding = "image/png"
	} else if strings.HasSuffix(avatar, ".jpg") {
		encoding = "image/jpeg"
	} else {
		return fmt.Errorf("FEED_AVATAR must be a png or jpg file")
	}
	reader, err := os.Open(avatar)
	if err != nil {
		return err
	}
	defer reader.Close()
	uploadApi := "com.atproto.repo.uploadBlob"
	var blob atproto.RepoUploadBlob_Output
	if err := Client.Do(ctx, xrpc.Procedure, encoding, uploadApi, nil, reader, &blob); err != nil {
		return err
	}

	record := bsky.FeedGenerator{
		Did:         UserDid.String(),
		DisplayName: config.FeedName,
		Description: &config.FeedDesc,
		Avatar:      blob.Blob,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	trueValue := true
	_, err = atproto.RepoPutRecord(ctx, Client, &atproto.RepoPutRecord_Input{
		Collection: "app.bsky.feed.generator",
		Record: &lex_util.LexiconTypeDecoder{
			Val: &record,
		},
		Repo:     UserDid.String(),
		Rkey:     "oneshot",
		Validate: &trueValue,
	})
	return err
}
