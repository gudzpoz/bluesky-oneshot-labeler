package database

import (
	"bluesky-oneshot-labeler/internal/config"
	"database/sql"
	_ "embed"
	"log/slog"
	"os"
	"strconv"
	"strings"

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

// Close closes the database connection.
// It logs a message indicating the disconnection from the specific database.
// If the connection is successfully closed, it returns nil.
// If an error occurs while closing the connection, it returns the error.
func (s *Service) Close() error {
	s.log.Info("Disconnected from database", "database", databaseFile)
	return s.db.Close()
}
