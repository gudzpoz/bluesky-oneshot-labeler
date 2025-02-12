package listener

import (
	"context"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/pemistahl/lingua-go"
)

type feedFilter func(*bsky.FeedPost) bool

// Customize these to filter out unwanted posts
var feedFilters = []feedFilter{
	IsNotComment,                 // filter out comments
	IsLangs("zh"),                // only show posts of certain languages
	IsLinguaLang(lingua.Chinese), // handle mis-classified posts from IsLang
	MaxTagCount(8),               // filter out posts with too many tags (probably spams)
	HasNoTags("nsfw"),            // filter out posts with a certain tag (case-insensitive)
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

type costlyfeedFilter func(ctx context.Context, post *bsky.FeedPost, did string) bool

// These filters are more expensive and are called only if the other filters pass
var costlyFeedFilters = []costlyfeedFilter{
	// // NsfwVitFilter("<url>", nsfwThreshold, minDiff, maxConns):
	// //   - <image> -> send to <url> -> produces { nsfw, sfw } scores
	// //   - if nsfw > nsfwThreshold && nsfw - sfw > minDiff, filter out
	// //   - maxConns is the max number of concurrent requests.
	// NsfwVitFilter("http://localhost:5000", 1.8, 1.2, 4),
}
