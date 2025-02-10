package database

import (
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
