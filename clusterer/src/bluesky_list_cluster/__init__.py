import dataclasses
import itertools
import json
import logging
import sqlite3
from pathlib import Path

from atproto import Client, models


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)
_info = _logger.info


@dataclasses.dataclass
class Config:
    user: str
    password: str
    session_file: str
    cache_db: str
    max_followers: int
    depth: int

    @classmethod
    def load(cls, config_file: str):
        with open(config_file, 'r') as f:
            return cls(**json.load(f))


class BlueskyListCluster(Client):
    config: Config

    cache_db: sqlite3.Connection

    list_uri: list[str]

    def __init__(self, config_file: str, list_uris: list[str]) -> None:
        super().__init__()
        self.list_uris = list_uris
        config = Config.load(config_file)
        self.config = config
        config_dir = Path(config_file).parent
        session_file = config_dir / config.session_file
        if session_file.exists():
            with open(session_file, 'r') as f:
                user = self.login(session_string=f.read())
        else:
            user = self.login(config.user, config.password)
            with open(session_file, 'w') as f:
                f.write(self.export_session_string())
        _info('Logged in as %s', user.handle)
        self.cache_db = sqlite3.connect(config_dir / config.cache_db)
        self._init_cache_db()

    def _init_cache_db(self):
        statements = [
            'CREATE TABLE IF NOT EXISTS list_uri (did TEXT PRIMARY KEY, fetched INTEGER)',
            '''CREATE TABLE IF NOT EXISTS user (
                did TEXT PRIMARY KEY,
                handle TEXT,
                display_name TEXT,
                description TEXT,
                follower_count INTEGER,
                following_count INTEGER,
                depth INTEGER
            )
            ''',
            '''CREATE TABLE IF NOT EXISTS follow_graph (
                follower_did TEXT,
                following_did TEXT,
                PRIMARY KEY (follower_did, following_did)
            )
            '''
        ]
        for statement in statements:
            self.cache_db.execute(statement)
        self.cache_db.commit()

    def _get_cached_list_size(self, uri: str) -> int | None:
        cursor = self.cache_db.execute('SELECT fetched FROM list_uri WHERE did = ?', (uri,))
        row = cursor.fetchone()
        if row:
            return row[0]
        return None

    def _update_list(self, uri: str):
        cursor: str | None = None
        items: list[models.AppBskyGraphDefs.ListItemView] = []
        cached_size = self._get_cached_list_size(uri)
        while True:
            part = self.app.bsky.graph.get_list(models.AppBskyGraphGetList.Params(
                list=uri,
                cursor=cursor,
                limit=50,
            ))
            if len(items) == 0:
                if part.list.list_item_count == cached_size:
                    # No new items
                    return
            cursor = part.cursor
            items.extend(part.items)
            if cursor is None:
                break

        existent_dids: set[str] = set()
        for batch in itertools.batched(items, 512):
            c = self.cache_db.execute(f'''SELECT did FROM user WHERE did IN ({
                ",".join(["?"] * len(batch))
            })''', [u.subject.did for u in batch])
            existent_dids.update(row[0] for row in c.fetchall())

        for user in items:
            did = user.subject.did
            if did in existent_dids:
                self.cache_db.execute(
                    'UPDATE user SET display_name = ?, description = ?, depth = 0 WHERE did = ?',
                    (user.subject.display_name, user.subject.description, did),
                )
            else:
                self.cache_db.execute(
                    'INSERT INTO user (did, handle, display_name, description, follower_count, following_count, depth) '
                    'VALUES (?, ?, ?, ?, ?, ?, 0)',
                    (did, user.subject.handle, user.subject.display_name, user.subject.description, -1, -1),
                )

        if cached_size is None:
            self.cache_db.execute('INSERT INTO list_uri (did, fetched) VALUES (?, ?)', (uri, len(items)))
        else:
            self.cache_db.execute('UPDATE list_uri SET fetched = ? WHERE did = ?', (len(items), uri))
        self.cache_db.commit()

    def _update_users(self):
        c = self.cache_db.execute(
            'SELECT did FROM user WHERE follower_count = -1 OR following_count = -1 AND depth <= ?',
            (self.config.depth,),
        )
        for rows in itertools.batched(c, 25):
            dids = [row[0] for row in rows]
            users = self.get_profiles(dids).profiles
            self.cache_db.executemany(
                'UPDATE user SET follower_count = ?, following_count = ? WHERE did = ?',
                [
                    (u.followers_count, u.follows_count, u.did)
                    for u in users
                ],
            )
            self.cache_db.commit()

    def update_all(self):
        _info('Updating all lists')
        for list_uri in self.list_uris:
            self._update_list(list_uri)

        _info('Updating all users')
        self._update_users()

    def close(self):
        self.cache_db.close()

