package database

import (
	"database/sql"
	"fmt"
	"strings"
)

func (s *Service) prepareLabelStatements() error {
	stmt, err := s.wdb.Prepare(
		"INSERT INTO user (did) VALUES (?)" +
			" ON CONFLICT (did) DO UPDATE SET uid = uid RETURNING uid",
	)
	if err != nil {
		return err
	}
	s.insertUserStmt = stmt

	stmt, err = s.wdb.Prepare(
		`INSERT INTO upstream_stats (uid, kind, count)
			VALUES (?, ?, 1)
		ON CONFLICT (uid, kind) DO UPDATE
			SET count = count + 1
		RETURNING id, count
		`,
	)
	if err != nil {
		return err
	}
	s.incrementCounterStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT sum(count) FROM upstream_stats WHERE uid = ?",
	)
	if err != nil {
		return err
	}
	s.labeledCountSumStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT count(*) FROM blocked_user JOIN user ON user.uid = blocked_user.uid WHERE user.did = ?",
	)
	if err != nil {
		return err
	}
	s.userBlockedStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT id FROM blocked_user ORDER BY id DESC LIMIT 1",
	)
	if err != nil {
		return err
	}
	s.lastBlockIdStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT blocked_user.id, user.did FROM blocked_user JOIN user ON user.uid = blocked_user.uid WHERE b.id > ? AND b.id <= ?",
	)
	if err != nil {
		return err
	}
	s.getBlockSinceStmt = stmt

	stmt, err = s.rdb.Prepare(
		"INSERT INTO blocked_user (uid) VALUES (?) ON CONFLICT DO NOTHING",
	)
	if err != nil {
		return err
	}
	s.insertBlockStmt = stmt

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

func (s *Service) IncrementCounter(uid int64, kind int) (int64, int64, error) {
	var id, count int64
	err := s.incrementCounterStmt.QueryRow(uid, kind).Scan(&id, &count)
	return id, count, err
}

func (s *Service) TotalCounts(uid int64) (int64, error) {
	var count int64
	err := s.labeledCountSumStmt.QueryRow(uid).Scan(&count)
	return count, err
}

func (s *Service) LastBlockId() (int64, error) {
	var id int64
	err := s.lastBlockIdStmt.QueryRow().Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (s *Service) IsUserBlocked(did string) (bool, error) {
	var count int64
	err := s.userBlockedStmt.QueryRow(did).Scan(&count)
	return count > 0, err
}

func (s *Service) GetBlocksSince(from, to int64) ([]string, int64, error) {
	rows, err := s.getBlockSinceStmt.Query(from, to)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var dids []string
	var id int64
	for rows.Next() {
		var did string
		err := rows.Scan(&id, &did)
		if err != nil {
			return nil, 0, err
		}
		dids = append(dids, did)
	}
	return dids, id, rows.Err()
}

func (s *Service) InsertBlock(uid int64) error {
	_, err := s.insertBlockStmt.Exec(uid)
	return err
}
