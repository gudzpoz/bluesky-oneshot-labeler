import dataclasses
import datetime
import itertools
import json
import logging
import sqlite3
import threading
import time
import typing
from concurrent.futures import Future, ThreadPoolExecutor, wait
from pathlib import Path

import numpy as np
import pandas as pd

from atproto import Client
from atproto_client.models import (
    AppBskyActorDefs, AppBskyGraphGetList, AppBskyGraphDefs,
    ComAtprotoRepoCreateRecord,
)

from sknetwork.data import Dataset, from_edge_list
from sknetwork.ranking import PageRank


_logger = logging.getLogger(__name__)
_debug = _logger.debug
_info = _logger.info
_warn = _logger.warning
# _logger.setLevel(logging.DEBUG)


T = typing.TypeVar('T')
def logged_batch(msg: str, iterable: list[T], batch_size: int = 100):
    size = len(iterable)
    i = 0
    for batch in itertools.batched(iterable, batch_size):
        _info(f'{msg} ({i}/{size})')
        i += len(batch)
        yield batch


def logged_batch_wait(msg: str, tasks: list[Future], batch_size: int = 10):
    for batch in logged_batch(msg, tasks, batch_size):
        results = wait(batch, None, 'FIRST_EXCEPTION')
        for result in results.done:
            e = result.exception()
            if e is not None:
                raise e


@dataclasses.dataclass
class Config:
    user: str
    password: str
    session_file: str
    cache_db: str
    output_csv: str
    page_rank_damping: float
    rank_threshold: float
    rate_limit: int
    max_followers: int
    depth: int

    @classmethod
    def load(cls, config_file: str):
        with open(config_file, 'r') as f:
            return cls(**json.load(f))


class BlueskyListCluster(Client):
    config: Config

    config_dir: Path

    user: AppBskyActorDefs.ProfileViewDetailed

    cache_db: sqlite3.Connection

    db_lock: threading.Lock

    list_uris: list[str]

    def __init__(self, config_file: str, list_uris: list[str]) -> None:
        super().__init__()
        self.list_uris = list_uris
        config = Config.load(config_file)
        self.config = config
        config_dir = Path(config_file).parent
        self.config_dir = config_dir
        session_file = config_dir / config.session_file
        if session_file.exists():
            with open(session_file, 'r') as f:
                user = self.login(session_string=f.read())
        else:
            user = self.login(config.user, config.password)
            with open(session_file, 'w') as f:
                f.write(self.export_session_string())
        _info('Logged in as %s', user.handle)
        self.user = user
        self.cache_db = sqlite3.connect(config_dir / config.cache_db, check_same_thread=False)
        self.db_lock = threading.Lock()
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
            'CREATE INDEX IF NOT EXISTS graph_depth_index ON user (depth)',
            '''CREATE TABLE IF NOT EXISTS follow_graph (
                follower_did TEXT,
                following_did TEXT,
                PRIMARY KEY (follower_did, following_did)
            )
            ''',
            'CREATE INDEX IF NOT EXISTS followed_by_index ON follow_graph (following_did)',
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
        items: list[AppBskyGraphDefs.ListItemView] = []
        cached_size = self._get_cached_list_size(uri)
        while True:
            part = self.app.bsky.graph.get_list(AppBskyGraphGetList.Params(
                list=uri,
                cursor=cursor,
                limit=50,
            ))
            if len(items) == 0:
                if part.list.list_item_count == cached_size:
                    # No new items
                    _debug('No new items in list %s (size: %d == %d)', uri, part.list.list_item_count, cached_size)
                    return
                _debug('New items in list %s (size: %d != %d)', uri, part.list.list_item_count, cached_size)
                cached_size = part.list.list_item_count
            cursor = part.cursor
            items.extend(part.items)
            if cursor is None:
                break
        _debug('Fetched %d items from list %s', len(items), uri)
        self._save_users([item.subject for item in items], 0)

        if cached_size is None:
            self.cache_db.execute('INSERT INTO list_uri (did, fetched) VALUES (?, ?)', (uri, cached_size))
        else:
            self.cache_db.execute('UPDATE list_uri SET fetched = ? WHERE did = ?', (cached_size, uri))
        self.cache_db.commit()

    def _get_existent_dids(self, dids: list[str]) -> list[str]:
        c = self.cache_db.execute(f'''SELECT did FROM user WHERE did IN ({
            ",".join(["?"] * len(dids))
        })''', dids)
        return [row[0] for row in c.fetchall()]

    def _save_users(self, users: list[AppBskyActorDefs.ProfileView] | list[AppBskyActorDefs.ProfileViewDetailed], depth: int):
        existent_dids: set[str] = set()
        for batch in itertools.batched(users, 512):
            existent_dids.update(self._get_existent_dids([u.did for u in batch]))

        for user in users:
            did = user.did
            if did in existent_dids:
                self.cache_db.execute(
                    'UPDATE user SET display_name = ?, description = ?, depth = ? WHERE did = ? AND depth >= ?',
                    (user.display_name, user.description, depth, did, depth),
                )
            else:
                self.cache_db.execute(
                    'INSERT INTO user (did, handle, display_name, description, follower_count, following_count, depth) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (did, user.handle, user.display_name, user.description, -1, -1, depth),
                )
        self.cache_db.commit()

    def _get_followship(
            self,
            method: typing.Literal['following', 'followers'],
            did: str,
            depth: int,
        ) -> list[AppBskyActorDefs.ProfileView]:
        users: list[AppBskyActorDefs.ProfileView] = []
        user: AppBskyActorDefs.ProfileView | None = None
        cursor: str | None = None
        while True:
            for i in range(3): # retry 3 times
                try:
                    if method == 'followers':
                        res = self.get_followers(did, cursor, limit=100)
                        next_users = res.followers
                    else:
                        res = self.get_follows(did, cursor, limit=100)
                        next_users = res.follows
                    break
                except Exception as e:
                    _warn('Retrying (%d/3) due to %s', i + 1, e)
                    time.sleep(1)
            else:
                raise Exception('Failed to get %s of %s', method, did)
            user = res.subject
            cursor = res.cursor
            users.extend(next_users)
            if cursor is None:
                break
            with self.db_lock:
                self._save_users(next_users, depth + 1)
        assert user is not None
        _debug('Followship (%s) for %s: %d users', method, did, len(users))
        return users

    def _update_old_users(self):
        dids: set[str] = set()
        c = self.cache_db.execute('SELECT DISTINCT follower_did FROM follow_graph')
        dids.update(row[0] for row in c)
        c = self.cache_db.execute('SELECT DISTINCT following_did FROM follow_graph')
        dids.update(row[0] for row in c)

        missing_dids: list[str] = []
        for batch in itertools.batched(dids, 512):
            missing_dids.extend(set(batch) - set(self._get_existent_dids(list(batch))))

        for batch in logged_batch('Fetching user info', missing_dids, 25):
            users = self.get_profiles(list(batch)).profiles
            self._save_users(users, 1)

        _info('Purge old users')
        self.cache_db.execute(
            'UPDATE user SET following_count = -1 '
            'WHERE 0 < following_count AND following_count < ? AND did NOT IN (SELECT DISTINCT follower_did FROM follow_graph)',
            (self.config.max_followers,),
        )
        self.cache_db.execute(
            'UPDATE user SET follower_count = -1 '
            'WHERE 0 < follower_count AND follower_count < ? AND did NOT IN (SELECT DISTINCT following_did FROM follow_graph)',
            (self.config.max_followers,),
        )
        self.cache_db.commit()

    def _fetch_followship(
            self,
            user: AppBskyActorDefs.ProfileViewDetailed,
            depth: int,
            counts: tuple[int, int]
        ):
        record_followers, record_following = counts
        followers_count = user.followers_count
        if followers_count is None or followers_count > self.config.max_followers:
            # Probably an account like @bsky.app that gets followed by everyone
            pass
        elif followers_count > 0 and record_followers == -1:
            followers = self._get_followship('followers', user.did, depth)
            with self.db_lock:
                self.cache_db.executemany(
                    'INSERT OR IGNORE INTO follow_graph (follower_did, following_did) VALUES (?, ?)',
                    [
                        (f.did, user.did)
                        for f in followers
                    ],
                )
                self.cache_db.commit()
        follows_count = user.follows_count
        if follows_count is None or follows_count > self.config.max_followers:
            # A follow bot?
            pass
        elif follows_count > 0 and record_following == -1:
            following = self._get_followship('following', user.did, depth)
            with self.db_lock:
                self.cache_db.executemany(
                    'INSERT OR IGNORE INTO follow_graph (follower_did, following_did) VALUES (?, ?)',
                    [
                        (user.did, f.did)
                        for f in following
                    ],
                )
                self.cache_db.commit()
        with self.db_lock:
            _debug(
                'Updating user info for %s (%d, %d) from (%d, %d)',
                user.did, user.followers_count, user.follows_count, record_followers, record_following,
            )
            self.cache_db.execute(
                'UPDATE user SET follower_count = ?, following_count = ? WHERE did = ?',
                (user.followers_count or 0, user.follows_count or 0, user.did),
            )
            self.cache_db.commit()

    def _update_users(self, depth: int):
        c = self.cache_db.execute(
            'SELECT did, follower_count, following_count FROM user WHERE (follower_count = -1 OR following_count = -1) AND depth = ?',
            (depth,),
        )
        with ThreadPoolExecutor(10) as executor:
            tasks: list[Future] = []
            for rows in logged_batch('Fetching user profiles', c.fetchall(), 25):
                dids = [row[0] for row in rows]
                did_counts = {row[0]: (row[1], row[2]) for row in rows}
                users = self.get_profiles(dids).profiles
                for user in users:
                    tasks.append(executor.submit(self._fetch_followship, user, depth, did_counts[user.did]))
                if len(tasks) >= 100:
                    logged_batch_wait('Fetching user followships', tasks)
                    tasks = []
            logged_batch_wait('Fetching user followships', tasks)
        self._update_depth(depth)

    def _update_depth(self, depth: int):
        self.cache_db.execute(
            '''
            UPDATE user SET depth = ? WHERE
            (
                did IN (
                    SELECT DISTINCT f.follower_did
                    FROM follow_graph f
                    JOIN user u ON u.did = f.following_did
                    WHERE u.depth = ?
                ) OR did IN (
                    SELECT DISTINCT f.following_did
                    FROM follow_graph f
                    JOIN user u ON u.did = f.follower_did
                    WHERE u.depth = ?
                )
            )
            AND depth > ?
            ''',
            (depth + 1, depth, depth, depth),
        )
        self.cache_db.commit()

    def update_all(self):
        for i, list_uri in enumerate(self.list_uris, 1):
            _info('Updating all lists: %d/%d (%s)', i, len(self.list_uris), list_uri)
            self._update_list(list_uri)

        _info('Updating old users')
        self._update_old_users()

        for depth in range(self.config.depth):
            _info('Updating users at depth %d', depth)
            self._update_users(depth)

    def rank_all(self):
        in_list_dids: set[str] = set(row[0] for row in self.cache_db.execute(
            'SELECT DISTINCT did FROM user WHERE depth = 0',
        ))
        graph = typing.cast(Dataset, from_edge_list(
            self.cache_db.execute('SELECT follower_did, following_did FROM follow_graph').fetchall(),
            directed=False,
        ))
        adjacency = graph.adjacency
        dids = graph.names
        weights: dict[int, float] = {}
        for i, did in enumerate(dids):
            weights[i] = 1.0 if did in in_list_dids else 0.1
        page_rank = PageRank(damping_factor=self.config.page_rank_damping)
        page_rank.fit(adjacency, weights=weights)
        scores = page_rank.predict()

        indices = np.argsort(scores)[::-1]
        scores = scores[indices]
        dids = np.array(dids)[indices]
        info = {
            row[0]: row[1:]
            for row in self.cache_db.execute('SELECT did, depth, handle, display_name, description FROM user')
        }
        df = pd.DataFrame(
            [(score, *info[did], did) for score, did in zip(scores, dids)],
            columns=['score', 'depth', 'handle', 'name', 'description', 'did'],
        )
        df.to_csv(self.config.output_csv, index=False)
        return list(dids[np.logical_and(
            scores > self.config.rank_threshold,
            df['depth'] != 0,
        )])

    def _add_one_to_list(self, did: str):
        record = ComAtprotoRepoCreateRecord.Data(
            collection='app.bsky.graph.listitem',
            repo=self.user.did,
            record={
                '$type': 'app.bsky.graph.listitem',
                'createdAt': datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'list': self.list_uris[0],
                'subject': did,
            },
        )
        self.com.atproto.repo.create_record(record)

    def add_to_list(self, dids: list[str]):
        with ThreadPoolExecutor(max_workers=25) as executor:
            tasks: list[Future] = []
            for did in dids[:self.config.rate_limit]:
                tasks.append(executor.submit(self._add_one_to_list, did))
            logged_batch_wait('Adding candids to list', tasks)

    def close(self):
        self.cache_db.close()

