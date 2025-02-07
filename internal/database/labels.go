package database

import (
	"fmt"
	"strings"
)

func (s *Service) prepareIncrementCounter() error {
	stmt, err := s.db.Prepare(
		"INSERT INTO user (did) VALUES (?)" +
			" ON CONFLICT (did) DO UPDATE uid = uid RETURNING uid",
	)
	if err != nil {
		return err
	}
	s.insertUserStmt = stmt

	stmt, err = s.db.Prepare(
		"INSERT INTO user (uid, kind, cts, count) VALUES (?, ?, ?, 1)" +
			" ON CONFLICT (uid, kind) DO UPDATE SET count = count + 1" +
			" RETURNING count",
	)
	if err != nil {
		return err
	}
	s.incrementCounterStmt = stmt

	return nil
}

func (s *Service) GetUserId(did string) (int64, error) {
	if !strings.HasPrefix(did, "did:") {
		return 0, fmt.Errorf("invalid did: %s", did)
	}
	did = did[4:]
	var id int64
	err := s.insertUserStmt.QueryRow(did).Scan(&id)
	return id, err
}

func (s *Service) IncrementCounter(uid int64, kind int, unixMillis int64) (int64, error) {
	var count int64
	err := s.incrementCounterStmt.QueryRow(uid, kind, unixMillis).Scan(&count)
	return count, err
}
