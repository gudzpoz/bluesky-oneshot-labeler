package listener

import (
	"context"
	"time"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/jetstream/pkg/models"
	"github.com/pemistahl/lingua-go"
	"golang.org/x/text/language"
)

type feedFilter func(post *bsky.FeedPost, event *models.Event) bool

// Customize these to filter out unwanted posts
var feedFilters = []feedFilter{
	// filter out comments
	IsNotComment,
	// only show posts of certain languages (as is claimed by the author)
	IsLangs(language.Chinese, language.English),
	// handle mis-classified posts from IsLang by actually detecting content languages
	IsLinguaLangs(lingua.Chinese),

	// extract tags from post, necessary for HasNoTags, MaxTagCount, etc.
	ExtractTags,
	// filter out posts with too many tags (probably spams)
	MaxTagCount(7),
	// filter out posts with a certain tag (case-insensitive)
	HasNoTags("nsfw"),
	// filter out posts with invalid tags (implying the post is posted by a badly-written bot or
	// the author does not even bother to format the tags correctly)
	HasBadTags(2, false),

	// distinctive spam text (please be very specific to avoid false positives)
	Not(ContainsAnyText(
		"发布了一篇小红书笔记，快来看吧！",
	)),

	// rate-limits to 1 post per 2 minutes per user, allowing 3 posts per 2 minutes burst
	RateLimit(3, 2*time.Minute),
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
