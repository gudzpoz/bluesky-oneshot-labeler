package listener

import (
	"strings"
	"unicode"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/pemistahl/lingua-go"
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

func (l *JetstreamListener) ShouldKeepFeedItem(post *bsky.FeedPost) bool {
	for _, filter := range feedFilters {
		if !filter(post) {
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
