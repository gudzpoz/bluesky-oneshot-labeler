CREATE TABLE config (key text PRIMARY KEY, value text);

CREATE TABLE user (
  uid integer PRIMARY KEY AUTOINCREMENT,
  did text not null
);

CREATE UNIQUE INDEX user_did ON user (did);

CREATE TABLE block_list (
  id integer PRIMARY KEY AUTOINCREMENT,
  uid integer not null,
  kind integer not null,
  cts integer not null,
  count integer not null
);

CREATE UNIQUE INDEX block_list_uid_kind ON block_list (uid, kind);
