package database

import (
	"database/sql"
	"time"
)

func (s *Service) prepareFeedStatements() error {
	stmt, err := s.db.Prepare(
		"INSERT INTO feed_list (uri, cts) VALUES (?, ?)",
	)
	if err != nil {
		return err
	}
	s.insertFeedItemStmt = stmt

	stmt, err = s.db.Prepare(
		"SELECT id, uri FROM feed_list WHERE id < ? ORDER BY id DESC LIMIT ?",
	)
	if err != nil {
		return err
	}
	s.getFeedItemsStmt = stmt

	stmt, err = s.db.Prepare(
		"SELECT id FROM feed_list WHERE cts >= ? ORDER BY id ASC LIMIT 1",
	)
	if err != nil {
		return err
	}
	s.scanFirstRecentIdStmt = stmt

	stmt, err = s.db.Prepare(
		"DELETE FROM feed_list WHERE id < ?",
	)
	if err != nil {
		return err
	}
	s.pruneFeedEntriesStmt = stmt

	stmt, err = s.db.Prepare(
		"PRAGMA incremental_vacuum",
	)
	if err != nil {
		return err
	}
	s.incrementalVacuumStmt = stmt

	return nil
}

func (s *Service) InsertFeedItem(uri string) error {
	_, err := s.insertFeedItemStmt.Exec(uri, time.Now().UTC().UnixMilli())
	return err
}

func (s *Service) GetFeedItems(cursor *int64, limit int) ([]string, error) {
	uris := make([]string, 0, limit)
	rows, err := s.getFeedItemsStmt.Query(*cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var uri string
		if err := rows.Scan(cursor, &uri); err != nil {
			return nil, err
		}
		uris = append(uris, uri)
	}
	return uris, nil
}

func (s *Service) PruneFeedEntries(before time.Time) error {
	// To reduce SQLite pressure, we do not have a cts index on purpose,
	// so we need to batch-delete by primary key id instead of cts.

	// Find the first id to preserve (cts >= before)
	var approxId int64
	if err := s.scanFirstRecentIdStmt.QueryRow(before.UnixMilli()).Scan(&approxId); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	_, err := s.pruneFeedEntriesStmt.Exec(approxId)
	return err
}

func (s *Service) IncrementalVacuum() error {
	_, err := s.incrementalVacuumStmt.Exec()
	return err
}
