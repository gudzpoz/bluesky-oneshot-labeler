package config

import (
	"log"
	"os"
	"strconv"

	_ "github.com/joho/godotenv/autoload"
)

func getEnvInt(s string) int {
	s = os.Getenv(s)
	if s == "" {
		log.Fatalf("Environment variable %s is not set", s)
		return 0
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("Environment variable %s is not a valid integer: %v", s, err)
	}
	return i
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

	OffenderThreshold = getEnvInt("OFFENDER_THRESHOLD")

	Socks5 = os.Getenv("SOCKS5")

	PlcToken = os.Getenv("PLC_TOKEN")

	FeedName   = os.Getenv("FEED_NAME")
	FeedAvatar = os.Getenv("FEED_AVATAR")
	FeedDesc   = os.Getenv("FEED_DESCRIPTION")
)
