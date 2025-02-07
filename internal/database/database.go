package database

import (
	"bluesky-oneshot-labeler/internal/config"
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Service struct {
	db  *sql.DB
	log *slog.Logger

	insertUserStmt       *sql.Stmt
	incrementCounterStmt *sql.Stmt
}

var dbInstance *Service
var databaseFile = config.DatabaseFile

func Init(logger *slog.Logger) error {
	url := databaseFile
	if url == "" {
		url = ":memory:"
	}

	initDb := false
	if url == ":memory:" {
		initDb = true
	} else {
		if _, err := os.Stat(url); os.IsNotExist(err) {
			initDb = true
		}
		url = "file:" + url + "?mode=rwc&_journal=WAL&_timeout=5000"
	}

	db, err := sql.Open("sqlite3", url)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	dbInstance = &Service{
		db:  db,
		log: logger,
	}
	if initDb {
		err = dbInstance.init()
		if err != nil {
			return err
		}
	}

	if err = dbInstance.upgrade(); err != nil {
		return err
	}

	err = dbInstance.prepareIncrementCounter()
	if err != nil {
		return err
	}

	return nil
}

func Instance() *Service {
	// Reuse Connection
	return dbInstance
}

//go:embed schema.sql
var schemaSql string
var dbVersion = 0

func (s *Service) init() error {
	for _, line := range strings.Split(schemaSql, ";") {
		_, err := s.db.Exec(line)
		if err != nil {
			return err
		}
	}
	return s.SetConfig("dbversion", strconv.Itoa(dbVersion))
}

func (s *Service) upgrade() error {
	verStr, err := s.GetConfig("dbversion", "0")
	if err != nil {
		return err
	}
	ver, err := strconv.Atoi(verStr)
	if err != nil {
		return err
	}

	switch ver {
	case 0:
		s.log.Debug("No upgrade needed")
		break
	default:
		s.log.Error("Unknown database version", "version", ver)
		os.Exit(1)
	}

	if ver < dbVersion {
		return s.SetConfig("dbversion", strconv.Itoa(dbVersion))
	}
	return nil
}

func (s *Service) GetConfig(key string, defaultValue string) (string, error) {
	var value string
	err := s.db.QueryRow("SELECT value FROM config WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return defaultValue, nil
	}
	return value, err
}

func (s *Service) SetConfig(key string, value string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM config WHERE key = ?", key)
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO config (key, value) VALUES (?, ?)", key, value)
	return err
}

func (s *Service) GetConfigInt(key string, defaultValue int64) (int64, error) {
	valueStr, err := s.GetConfig(key, "")
	if err != nil {
		return 0, err
	}
	if valueStr == "" {
		return defaultValue, nil
	}
	return strconv.ParseInt(valueStr, 10, 64)
}

func (s *Service) SetConfigInt(key string, value int64) error {
	return s.SetConfig(key, strconv.FormatInt(value, 10))
}

// Health checks the health of the database connection by pinging the database.
// It returns a map with keys indicating various health statistics.
func (s *Service) Health() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stats := make(map[string]string)

	// Ping the database
	err := s.db.PingContext(ctx)
	if err != nil {
		stats["status"] = "down"
		stats["error"] = fmt.Sprintf("db down: %v", err)
		s.log.Error("Database down", "err", err)
		os.Exit(1)
		return stats
	}

	// Database is up, add more statistics
	stats["status"] = "up"
	stats["message"] = "It's healthy"

	// Get database stats (like open connections, in use, idle, etc.)
	dbStats := s.db.Stats()
	stats["open_connections"] = strconv.Itoa(dbStats.OpenConnections)
	stats["in_use"] = strconv.Itoa(dbStats.InUse)
	stats["idle"] = strconv.Itoa(dbStats.Idle)
	stats["wait_count"] = strconv.FormatInt(dbStats.WaitCount, 10)
	stats["wait_duration"] = dbStats.WaitDuration.String()
	stats["max_idle_closed"] = strconv.FormatInt(dbStats.MaxIdleClosed, 10)
	stats["max_lifetime_closed"] = strconv.FormatInt(dbStats.MaxLifetimeClosed, 10)

	// Evaluate stats to provide a health message
	if dbStats.OpenConnections > 40 { // Assuming 50 is the max for this example
		stats["message"] = "The database is experiencing heavy load."
	}

	if dbStats.WaitCount > 1000 {
		stats["message"] = "The database has a high number of wait events, indicating potential bottlenecks."
	}

	if dbStats.MaxIdleClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many idle connections are being closed, consider revising the connection pool settings."
	}

	if dbStats.MaxLifetimeClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many connections are being closed due to max lifetime, consider increasing max lifetime or revising the connection usage pattern."
	}

	return stats
}

// Close closes the database connection.
// It logs a message indicating the disconnection from the specific database.
// If the connection is successfully closed, it returns nil.
// If an error occurs while closing the connection, it returns the error.
func (s *Service) Close() error {
	s.log.Info("Disconnected from database", "database", databaseFile)
	return s.db.Close()
}
