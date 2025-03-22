package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"unicode"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/pemistahl/lingua-go"
	"golang.org/x/sync/semaphore"
	"golang.org/x/text/language"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func Not(filter feedFilter) feedFilter {
	return func(post *bsky.FeedPost) bool {
		return !filter(post)
	}
}

func IsNotComment(post *bsky.FeedPost) bool {
	return post.Reply == nil
}

func IsLangs(langs ...language.Tag) feedFilter {
	matcher := language.NewMatcher(langs)
	return func(post *bsky.FeedPost) bool {
		for _, lang := range post.Langs {
			tag, err := language.Parse(lang)
			if err != nil {
				continue
			}
			_, _, confidence := matcher.Match(tag)
			if confidence != language.No {
				return true
			}
		}
		return false
	}
}

func ExtractTags(post *bsky.FeedPost) bool {
	for _, facet := range post.Facets {
		for _, feature := range facet.Features {
			tag := feature.RichtextFacet_Tag
			if tag != nil {
				post.Tags = append(post.Tags, tag.Tag)
			}
		}
	}
	return true
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
	return Not(HasAnyTag(tags...))
}

func MaxTagCount(max int) feedFilter {
	return func(post *bsky.FeedPost) bool {
		return len(post.Tags) <= max
	}
}

// So... there are lots of bots (or spammers) that post with tons of invalid tags,
// which seem to be a good indicator of spam.
//
// Specifically, tags like:
// - "#tag1#tag2#tag3" (as *a single tag*)
// - "#tag" (as plain text)
// are common in such posts, and this filter will filter out these.
func HasBadTags(maxHashesInTag int, allowNonTagHashes bool) feedFilter {
	// https://github.com/bluesky-social/atproto/blob/main/packages/api/src/rich-text/util.ts
	chars := `\x{00AD}\x{2060}\x{200A}-\x{200D}\x{20E2}\x{FE0F}`
	hashRegExp, err := regexp.Compile(fmt.Sprintf(`(^|\s)[#＃]([^\s\x%s]*[^\d\s\p{P}%s]+[^\s%s]*)?`, chars, chars, chars))
	if err != nil {
		slog.Error("failed to compile hash regexp", "err", err)
		allowNonTagHashes = true
	}
	return func(post *bsky.FeedPost) bool {
		for _, tag := range post.Tags {
			hashes := strings.Count(tag, "#")
			if hashes > maxHashesInTag {
				return false
			}
		}
		if allowNonTagHashes || len(post.Tags) != 0 || !strings.Contains(post.Text, "#") {
			return true
		}
		return !hashRegExp.MatchString(post.Text)
	}
}

func IsLinguaLangs(expected ...lingua.Language) feedFilter {
	langSet := make(map[lingua.Language]struct{})
	wantsChinese := false
	for _, lang := range expected {
		langSet[lang] = struct{}{}
		if lang == lingua.Chinese {
			wantsChinese = true
		}
	}

	return func(post *bsky.FeedPost) bool {
		text := getPostText(post)
		hasJapanese := false
		langs := langDetector.DetectMultipleLanguagesOf(text)
		for _, lang := range langs {
			if _, ok := langSet[lang.Language()]; ok {
				return true
			}
			if wantsChinese && lang.Language() == lingua.Japanese {
				hasJapanese = true
			}
		}
		if wantsChinese && hasJapanese {
			// Lingua mis-detections: https://github.com/pemistahl/lingua-go/issues/38
			return hasChinese(text)
		}
		return false
	}
}

func noop(post *bsky.FeedPost) bool { return true }

func ContainsAnyText(texts ...string) feedFilter {
	if len(texts) == 0 {
		return noop
	}
	for i, text := range texts {
		texts[i] = regexp.QuoteMeta(text)
	}
	matcher, err := regexp.Compile("(?i)" + strings.Join(texts, "|"))
	if err != nil {
		slog.Error("failed to compile text regexp", "err", err)
		return noop
	}
	return func(post *bsky.FeedPost) bool {
		return matcher.MatchString(post.Text)
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

func NsfwVitFilter(upstream string, nsfwThreshold, minDiff float64, maxConns int) costlyfeedFilter {
	nsfwLogger := slog.Default().WithGroup("nsfw-vit")
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

func (l *JetstreamListener) ShouldKeepFeedItem(post *bsky.FeedPost) bool {
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
