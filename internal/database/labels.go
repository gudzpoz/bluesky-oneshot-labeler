package database

import (
	"database/sql"
	"fmt"
	"strings"
)

func (s *Service) prepareLabelStatements() error {
	stmt, err := s.db.Prepare(
		"INSERT INTO user (did) VALUES (?)" +
			" ON CONFLICT (did) DO UPDATE SET uid = uid RETURNING uid",
	)
	if err != nil {
		return err
	}
	s.insertUserStmt = stmt

	stmt, err = s.db.Prepare(
		"INSERT INTO block_list (uid, kind, cts, count) VALUES (?, ?, ?, 1)" +
			" ON CONFLICT (uid, kind) DO UPDATE SET count = count + 1" +
			" RETURNING id, count",
	)
	if err != nil {
		return err
	}
	s.incrementCounterStmt = stmt

	stmt, err = s.db.Prepare(
		"SELECT id FROM block_list ORDER BY id DESC LIMIT 1",
	)
	if err != nil {
		return err
	}
	s.lastLabelIdStmt = stmt

	stmt, err = s.db.Prepare(
		"SELECT id, u.did, kind, cts FROM block_list l" +
			" JOIN user u ON l.uid = u.uid" +
			" WHERE ? < id AND id <= ?" +
			" ORDER BY id ASC",
	)
	if err != nil {
		return err
	}
	s.queryLabelsSinceStmt = stmt

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

type Pair struct {
	Id    int64
	Count int64
}

func (s *Service) IncrementCounter(uid int64, kind int, unixMillis int64) (Pair, error) {
	var id, count int64
	err := s.incrementCounterStmt.QueryRow(uid, kind, unixMillis).Scan(&id, &count)
	return Pair{id, count}, err
}

type QueryLabelsInput struct {
	Cursor      int64    `json:"cursor"`
	Limit       int64    `json:"limit"`
	Sources     []string `json:"sources"`
	UriPatterns []string `json:"uriPatterns"`
}

type Label struct {
	Id   int64  `json:"id"`
	Did  string `json:"did"`
	Kind int    `json:"kind"`
	Cts  int64  `json:"cts"`
}

func (s *Service) QueryLabels(input *QueryLabelsInput) ([]Label, error) {
	params := make([]any, 0, len(input.UriPatterns)+2)
	params = append(params, input.Cursor)

	sql := strings.Builder{}
	sql.WriteString(
		"SELECT id, u.did, kind, cts FROM block_list l" +
			" JOIN user u ON l.uid = u.uid" +
			" WHERE id > ? ",
	)
	sql.WriteByte(' ')
	if input.UriPatterns != nil {
		sql.WriteString(" AND (")
		for i, pat := range input.UriPatterns {
			if i > 0 {
				sql.WriteString(" OR ")
			}
			sql.WriteString("u.did LIKE ?")
			params = append(params, pat)
		}
		sql.WriteByte(')')
	}
	sql.WriteString(" ORDER BY id ASC LIMIT ?")
	params = append(params, input.Limit)

	rows, err := s.db.Query(sql.String(), params...)
	if err != nil {
		return nil, err
	}

	results := make([]Label, 0, input.Limit)
	for rows.Next() {
		var id int64
		var did string
		var kind int
		var cts int64
		err := rows.Scan(&id, &did, &kind, &cts)
		if err != nil {
			rows.Close()
			return nil, err
		}
		results = append(results, Label{
			Id:   id,
			Did:  did,
			Kind: kind,
			Cts:  cts,
		})
	}

	return results, nil
}

func (s *Service) LatestLabelId() (int64, error) {
	var id int64
	err := s.lastLabelIdStmt.QueryRow().Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (s *Service) QueryLabelsSince(from int64, to int64) (*sql.Rows, error) {
	rows, err := s.queryLabelsSinceStmt.Query(from, to)
	return rows, err
}
