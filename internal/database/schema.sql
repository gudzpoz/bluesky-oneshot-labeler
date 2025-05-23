CREATE TABLE config (key text PRIMARY KEY, value text);

CREATE TABLE user (
  uid integer PRIMARY KEY AUTOINCREMENT,
  did text not null
);

CREATE UNIQUE INDEX user_did ON user (did);

CREATE TABLE upstream_stats (
  id integer PRIMARY KEY AUTOINCREMENT,
  uid integer not null,
  kind integer not null,
  count integer not null
);

CREATE UNIQUE INDEX block_list_uid_kind ON upstream_stats (uid, kind);

CREATE TABLE blocked_user (
  id integer PRIMARY KEY,
  uid integer not null
);

CREATE UNIQUE INDEX blocked_user_uid_id ON blocked_user (uid);

CREATE TABLE feed_list (
  id integer PRIMARY KEY AUTOINCREMENT,
  uri text not null,
  cts integer not null
);
