package database

func (s *Service) IncrementCounter(kind int, did string) (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT OR IGNORE INTO user_stats (kind, did, count) VALUES (?, ?, 0)",
		kind, did,
	)
	if err != nil {
		return 0, err
	}

	var count int
	err = tx.QueryRow(
		"UPDATE user_stats SET count = count + 1 WHERE kind = ? AND did = ? RETURNING count",
		kind, did,
	).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, tx.Commit()
}

func (s *Service) BlockUser(kind int, did string, unixMillis int64) error {
	_, err := s.db.Exec(
		"INSERT OR IGNORE INTO blocked_user (user_id, created_at) VALUES"+
			" ((SELECT user_id FROM user_stats WHERE kind = ? AND did = ?), ?)",
		kind, did, unixMillis,
	)
	return err
}
