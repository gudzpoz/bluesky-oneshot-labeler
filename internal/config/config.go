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
	Password = os.Getenv("PASSWORD")

	UpstreamUser = os.Getenv("UPSTREAM_USER")

	DatabaseFile = os.Getenv("DATABASE_FILE")
	SessionFile  = os.Getenv("SESSION_FILE")

	Port = getEnvInt("PORT")

	OffenderThreshold = getEnvInt("OFFENDER_THRESHOLD")
)
