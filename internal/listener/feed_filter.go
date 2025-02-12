package listener

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"unicode"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/pemistahl/lingua-go"
	"golang.org/x/sync/semaphore"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func IsNotComment(post *bsky.FeedPost) bool {
	return post.Reply == nil
}

func IsLangs(langs ...string) feedFilter {
	langSet := make(map[string]struct{})
	for _, lang := range langs {
		langSet[lang] = struct{}{}
	}
	return func(post *bsky.FeedPost) bool {
		for _, lang := range post.Langs {
			if _, ok := langSet[lang]; ok {
				return true
			}
		}
		return false
	}
}

var textNormalizer = transform.Chain(norm.NFKD, runes.Remove(runes.In(unicode.Mn)), norm.NFKC)

func normalizeText(text string) string {
	normalized, _, err := transform.String(textNormalizer, text)
	if err != nil {
		normalized = text
	}
	return strings.ToLower(normalized)
}

func HasAnyTag(tags ...string) feedFilter {
	tagSet := make(map[string]struct{})
	for _, tag := range tags {
		tagSet[normalizeText(tag)] = struct{}{}
	}
	return func(post *bsky.FeedPost) bool {
		for _, t := range post.Tags {
			t = normalizeText(t)
			if _, ok := tagSet[t]; ok {
				return true
			}
		}
		return false
	}
}

func HasNoTags(tags ...string) feedFilter {
	f := HasAnyTag(tags...)
	return func(post *bsky.FeedPost) bool {
		return !f(post)
	}
}

func MaxTagCount(max int) feedFilter {
	return func(post *bsky.FeedPost) bool {
		return len(post.Tags) <= max
	}
}

func IsLinguaLang(expected lingua.Language) feedFilter {
	return func(post *bsky.FeedPost) bool {
		text := getPostText(post)
		hasJapanese := false
		langs := langDetector.DetectMultipleLanguagesOf(text)
		for _, lang := range langs {
			if lang.Language() == expected {
				return true
			}
			if expected == lingua.Chinese && lang.Language() == lingua.Japanese {
				hasJapanese = true
			}
		}
		if expected == lingua.Chinese && hasJapanese {
			// Lingua mis-detections: https://github.com/pemistahl/lingua-go/issues/38
			return hasChinese(text)
		}
		return false
	}
}

func cdnBskyAppUrl(did string, blobRef *util.LexLink) string {
	return "https://cdn.bsky.app/img/feed_thumbnail/plain/" + did +
		"/" + blobRef.String() + "@jpeg"
}

type nsfwVitResult struct {
	Nsfw  float64 `json:"nsfw"`
	Sfw   float64 `json:"sfw"`
	Error string  `json:"error"`
}

var nsfwLogger = slog.Default().WithGroup("nsfw-vit")

func NsfwVitFilter(upstream string, nsfwThreshold, minDiff float64, maxConns int) costlyfeedFilter {
	limit := semaphore.NewWeighted(int64(maxConns))
	return func(ctx context.Context, post *bsky.FeedPost, did string) bool {
		if post.Embed == nil || post.Embed.EmbedImages == nil {
			return true
		}
		images := post.Embed.EmbedImages.Images
		imageUrls := make([]string, len(images))
		for i, img := range post.Embed.EmbedImages.Images {
			url := cdnBskyAppUrl(did, &img.Image.Ref)
			imageUrls[i] = url
		}

		if err := limit.Acquire(ctx, 1); err != nil {
			return true
		}
		defer limit.Release(1)

		req, err := http.NewRequestWithContext(
			ctx, "POST", upstream,
			strings.NewReader(strings.Join(imageUrls, "\n")),
		)
		if err != nil {
			nsfwLogger.Warn("failed to create NSFW filter request")
			return true
		}
		req.Header.Set("Content-Type", "text/plain")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			nsfwLogger.Warn("failed to query NSFW filter")
			return true
		}
		if res.StatusCode != http.StatusOK {
			nsfwLogger.Warn("failed to query NSFW filter", "status", res.Status)
			return true
		}
		defer res.Body.Close()
		var results []nsfwVitResult
		if err := json.NewDecoder(res.Body).Decode(&results); err != nil {
			nsfwLogger.Warn("failed to decode NSFW filter response")
			return true
		}
		for i, result := range results {
			if result.Error != "" {
				nsfwLogger.Warn("failed to query NSFW filter", "error", result.Error)
				continue
			}
			if result.Nsfw > nsfwThreshold && result.Nsfw-result.Sfw > minDiff {
				nsfwLogger.Debug("NSFW filter blocked post", "img", imageUrls[i])
				return false
			}
		}
		return true
	}
}

func (l *JetstreamListener) ShouldKeepFeedItem(post *bsky.FeedPost, did string) bool {
	for _, filter := range feedFilters {
		if !filter(post) {
			return false
		}
	}
	return true
}

func (l *JetstreamListener) ShouldKeepFeedItemCostly(ctx context.Context, post *bsky.FeedPost, did string) bool {
	for _, filter := range costlyFeedFilters {
		if !filter(ctx, post, did) {
			return false
		}
	}
	return true
}

func getPostText(post *bsky.FeedPost) string {
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
	return text
}
