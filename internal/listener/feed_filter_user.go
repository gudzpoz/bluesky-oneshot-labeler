package listener

import (
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/pemistahl/lingua-go"
)

type feedFilter func(*bsky.FeedPost) bool

var feedFilters []feedFilter = []feedFilter{
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
