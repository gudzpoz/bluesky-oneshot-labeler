package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/joho/godotenv/autoload"
)

func getEnvInt(s string) int {
	v := os.Getenv(s)
	if v == "" {
		log.Fatalf("Environment variable %s is not set", s)
		return 0
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("Environment variable %s is not a valid integer: %v", s, err)
	}
	return i
}

func getEnvFloat(s string) float64 {
	v := os.Getenv(s)
	if v == "" {
		log.Fatalf("Environment variable %s is not set", s)
		return 0
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Fatalf("Environment variable %s is not a valid float: %v", s, err)
	}
	return f
}

func getEnvList(s string) []string {
	list := strings.Split(os.Getenv(s), ",")
	for i := range list {
		list[i] = strings.TrimSpace(list[i])
	}
	return list
}

var (
	Username = os.Getenv("USERNAME")
	UserDid  = os.Getenv("USER_DID")
	Password = os.Getenv("PASSWORD")

	UpstreamUser = os.Getenv("UPSTREAM_USER")

	DatabaseFile = os.Getenv("DATABASE_FILE")
	SessionFile  = os.Getenv("SESSION_FILE")

	Host = os.Getenv("HOST")
	Port = getEnvInt("PORT")

	AppViewRateLimit = getEnvInt("APPVIEW_RATE_LIMIT")

	OffendingPostRatio = getEnvFloat("OFFENDING_POST_RATIO")

	Socks5 = os.Getenv("SOCKS5")

	PlcToken = os.Getenv("PLC_TOKEN")

	FeedName   = os.Getenv("FEED_NAME")
	FeedAvatar = os.Getenv("FEED_AVATAR")
	FeedDesc   = os.Getenv("FEED_DESCRIPTION")

	ExternalBlockList = os.Getenv("EXTERNAL_BLOCK_LIST")

	ModeratorHandles = getEnvList("MODERATOR_HANDLES")
)
