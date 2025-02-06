package config

import (
	"os"

	_ "github.com/joho/godotenv/autoload"
)

var (
	Username = os.Getenv("USERNAME")
	Password = os.Getenv("PASSWORD")

	DatabaseFile = os.Getenv("DATABASE_FILE")
	SessionFile  = os.Getenv("SESSION_FILE")

	Port = os.Getenv("PORT")

	BlockThreshold    = os.Getenv("BLOCK_THRESHOLD")
	OffenderThreshold = os.Getenv("OFFENDER_THRESHOLD")
)
