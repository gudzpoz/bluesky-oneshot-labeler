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
		`UPDATE block_list SET posts = jsonb_set(posts, concat('$.', json_quote(?)), jsonb('true'))
		WHERE uid = ? AND kind = ?`,
	)
	if err != nil {
		return err
	}
	s.updateCounterRecStmt = stmt

	stmt, err = s.wdb.Prepare(
		`INSERT INTO block_list (uid, kind, cts, count, posts)
			VALUES (?, ?, ?, 1, jsonb_object(?, jsonb('true')))
		ON CONFLICT (uid, kind) DO UPDATE
			SET count = count + 1,
				posts = jsonb_set(posts, concat('$.', json_quote(?)), jsonb('true'))
		RETURNING id, count
		`,
	)
	if err != nil {
		return err
	}
	s.incrementCounterStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT id FROM block_list ORDER BY id DESC LIMIT 1",
	)
	if err != nil {
		return err
	}
	s.lastLabelIdStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT id, u.did, kind, cts FROM block_list l" +
			" JOIN user u ON l.uid = u.uid" +
			" WHERE ? < id AND id <= ?" +
			" ORDER BY id ASC",
	)
	if err != nil {
		return err
	}
	s.queryLabelsSinceStmt = stmt

	stmt, err = s.rdb.Prepare(
		"SELECT count(*) FROM user WHERE did = ? LIMIT 1",
	)
	if err != nil {
		return err
	}
	s.userExistsStmt = stmt

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

func (s *Service) IncrementCounter(uid int64, kind int, rkey string, unixMillis int64) (Pair, error) {
	var id, count int64
	err := s.incrementCounterStmt.QueryRow(uid, kind, unixMillis, rkey, rkey).Scan(&id, &count)
	return Pair{id, count}, err
}

func (s *Service) UpdateCounterRec(uid int64, kind int, rkey string) error {
	_, err := s.updateCounterRecStmt.Exec(rkey, uid, kind)
	return err
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

	rows, err := s.rdb.Query(sql.String(), params...)
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

func (s *Service) IsUserLabeled(did string) (bool, error) {
	var count int64
	err := s.userExistsStmt.QueryRow(did).Scan(&count)
	return count > 0, err
}
