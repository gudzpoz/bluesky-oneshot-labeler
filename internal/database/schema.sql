CREATE TABLE config (key text PRIMARY KEY, value text);

CREATE TABLE user_stats (
  user_id integer PRIMARY KEY AUTOINCREMENT,
  kind integer,
  did text,
  count integer
);

CREATE UNIQUE INDEX blocked_user_did_per_kind ON user_stats (kind, did);

CREATE TABLE blocked_user (
  id integer PRIMARY KEY AUTOINCREMENT,
  user_id integer,
  created_at timestamp
);

CREATE UNIQUE INDEX blocked_user_id ON blocked_user (user_id);
