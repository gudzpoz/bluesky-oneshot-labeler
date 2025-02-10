package listener

import (
	"slices"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/pemistahl/lingua-go"
)

type feedFilter func(*bsky.FeedPost) bool

var feedFilters []feedFilter = []feedFilter{
	IsNotComment,
	IsLang("zh"),
	IsLinguaLang(lingua.Chinese),
	HasNoTag("NSFW"),
}

func IsNotComment(post *bsky.FeedPost) bool {
	return post.Reply == nil
}

func IsLang(lang string) feedFilter {
	return func(post *bsky.FeedPost) bool {
		return slices.Contains(post.Langs, "zh")
	}
}

func HasTag(tag ...string) feedFilter {
	tagSet := make(map[string]struct{})
	for _, t := range tag {
		tagSet[t] = struct{}{}
	}
	return func(post *bsky.FeedPost) bool {
		for _, t := range post.Tags {
			if _, ok := tagSet[t]; ok {
				return true
			}
		}
		return false
	}
}

func HasNoTag(tag string) feedFilter {
	return func(post *bsky.FeedPost) bool {
		return !slices.Contains(post.Tags, tag)
	}
}

// Used by IsLinguaLang
var linguaLanguages = []lingua.Language{
	lingua.Chinese,
	lingua.Japanese,
	lingua.Korean,
	lingua.English,
}
var langDetector = lingua.NewLanguageDetectorBuilder().
	FromLanguages(linguaLanguages...).
	WithPreloadedLanguageModels().
	Build()

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
