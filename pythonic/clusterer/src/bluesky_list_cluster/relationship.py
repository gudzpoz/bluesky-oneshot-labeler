import asyncio
import dataclasses
import logging
import typing

import aiolimiter
import aiosqlite
from atproto import AsyncClient, exceptions
from atproto_client.models.common import XrpcError
from tqdm.asyncio import tqdm_asyncio

from .config import Config


_logger = logging.getLogger(__name__)
_debug = _logger.debug
_info = _logger.info
_warn = _logger.warning


async def init_db(config: Config) -> 'RelDatabase':
    db = await aiosqlite.connect(config.cache_db_path)
    client = RelDatabase(db, config)
    refreshed = await client._init()
    if not refreshed:
        client = RelDatabase(db, config)
        await client._init(refresh=True)
    return client


T = typing.TypeVar('T')
def _batch(iterable: list[T], batch_size: int):
    for i in range(0, len(iterable), batch_size):
        yield iterable[i:i + batch_size]


@dataclasses.dataclass
class User:
    did: str
    handle: str
    nick: str
    desc: str


class RelDatabase(AsyncClient):
    cache_db: aiosqlite.Connection
    rate_limit: aiolimiter.AsyncLimiter
    config: Config

    def __init__(
        self,
        db: aiosqlite.Connection,
        config: Config,
    ):
        super().__init__()
        self.config = config
        self.cache_db = db
        self.rate_limit = aiolimiter.AsyncLimiter(config.rate_limit, 1)

    async def _init_cache_db(self):
        statements = [
            '''CREATE TABLE IF NOT EXISTS user (
                uid INTEGER PRIMARY KEY AUTOINCREMENT,
                did TEXT NOT NULL,
                handle TEXT NOT NULL,
                nick TEXT NOT NULL,
                desc TEXT NOT NULL,
                followers INTEGER NOT NULL,
                following INTEGER NOT NULL,
                fetched INTEGER NOT NULL
            )
            ''',
            'CREATE UNIQUE INDEX IF NOT EXISTS user_did_index ON user (did)',
            'CREATE INDEX IF NOT EXISTS user_handle_index ON user (handle)',

            '''CREATE TABLE IF NOT EXISTS follow_graph (
                from_uid INTEGER NOT NULL,
                to_uid INTEGER NOT NULL,
                PRIMARY KEY (from_uid, to_uid)
            )
            ''',
            'CREATE INDEX IF NOT EXISTS followed_by_index ON follow_graph (to_uid)',
        ]
        for statement in statements:
            await self.cache_db.execute(statement)
        await self.cache_db.commit()

    async def close(self):
        await self.cache_db.close()

    async def _init(self, refresh=False):
        session_file = self.config.session_file_path
        user = None
        if not refresh and session_file.exists():
            with open(session_file, 'r') as f:
                session_string = f.read()
            try:
                user = await self.login(session_string=session_string)
            except exceptions.BadRequestError:
                return False
        if user is None:
            user = await self.login(self.config.user, self.config.password)
        with open(session_file, 'w') as f:
            f.write(self.export_session_string())
        await self._init_cache_db()
        _info('Logged in as %s', user.handle)
        return True

    async def _get_existent_dids(self, dids: list[str]) -> list[tuple[str, int]]:
        '''Get the dids that already exist in the database.

        Note that due to SQLite supporting up to several hundred parameters in a single query,
        this function is limited to a maximum of 512 dids.'''
        assert len(dids) <= 512, 'Cannot query more than 512 dids at once'
        existent_dids = []
        async with self.cache_db.execute(f'''SELECT did, uid FROM user WHERE did IN ({
            ",".join(["?"] * len(dids))
        })''', dids) as c:
            async for row in c:
                existent_dids.append((row[0], row[1]))
        return existent_dids

    async def _fetch_users(self, dids: list[str]):
        '''Fetch users from the database or the network.

        Due to Bluesky spec, the maximum number of users that can be fetched
        in a single request is 25.'''
        assert len(dids) <= 25, 'Cannot fetch more than 25 users at once'
        async with self.rate_limit:
            users = await self.get_profiles(dids)
        for user in users.profiles:
            await self.cache_db.execute(
                '''
                INSERT OR IGNORE INTO user (did, handle, nick, desc, followers, following, fetched)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    user.did,
                    user.handle or '',
                    user.display_name or '',
                    user.description or '',
                    user.followers_count or 0,
                    user.follows_count or 0,
                    0,
                ),
            )
        await self.cache_db.commit()
        uids = []
        async with self.cache_db.execute(f'''SELECT uid FROM user WHERE did IN ({
            ",".join(["?"] * len(dids))
        })''', dids) as c:
            async for row in c:
                uids.append(row[0])
        return uids

    async def ensure_users(self, dids: list[str], tqdm=True) -> list[int]:
        uids = []
        missing_dids = []
        for dids_batch in _batch(dids, 512):
            existent_dids = await self._get_existent_dids(dids_batch)
            missing_dids.extend(set(dids_batch) - set(did for did, _ in existent_dids))
            uids.extend(uid for _, uid in existent_dids)

        if tqdm:
            uid_list = await tqdm_asyncio.gather(*[
                self._fetch_users(batch_dids)
                for batch_dids in _batch(missing_dids, 25)
            ])
        else:
            uid_list = await asyncio.gather(*[
                self._fetch_users(batch_dids)
                for batch_dids in _batch(missing_dids, 25)
            ], return_exceptions=True)
        for uid_list_batch in uid_list:
            if isinstance(uid_list_batch, BaseException):
                _warn('Failed to fetch users: %s', uid_list_batch)
            else:
                uids.extend(uid_list_batch)
        return uids

    async def _get_followship(
            self,
            method: typing.Literal['following', 'followers'],
            did: str,
        ) -> list[int]:
        users: list[int] = []
        cursor: str | None = None
        while True:
            for i in range(3): # retry 3 times
                try:
                    async with self.rate_limit:
                        if method == 'followers':
                            res = await self.get_followers(did, cursor, limit=100)
                            next_users = res.followers
                        else:
                            res = await self.get_follows(did, cursor, limit=100)
                            next_users = res.follows
                    break
                except exceptions.AtProtocolError as e:
                    if isinstance(e, exceptions.BadRequestError):
                        err = e.response and e.response.content
                        if (
                            isinstance(err, XrpcError)
                            and err.message
                            and 'Actor not found' in err.message
                        ):
                            _warn('Actor not found: %s', did)
                            return users
                    _warn('Retrying (%d/3) due to %s', i + 1, e)
            else:
                raise Exception('Failed to get %s of %s', method, did)
            cursor = res.cursor
            if cursor is None:
                break
            users.extend(await self.ensure_users([u.did for u in next_users], tqdm=False))
        _debug('Followship (%s) for %s: %d users', method, did, len(users))
        return users

    async def _mark_fetched(self, did: str, followers: int, following: int):
        _debug(
            'Updating user info for %s (%d, %d)',
            did, followers, following,
        )
        await self.cache_db.execute(
            'UPDATE user SET fetched = 1 WHERE did = ?',
            (did,),
        )
        await self.cache_db.commit()

    async def _fetch_user_graph(
            self,
            did: str,
            force: bool = False,
        ):
        async with self.cache_db.execute(
            'SELECT uid, followers, following, fetched FROM user WHERE did = ?',
            (did,),
        ) as c:
            record = await c.fetchone()
        if record is None:
            return False
        uid, followers, following, fetched = record
        if fetched and not force:
            return True

        if followers > 0 and followers < self.config.max_followers:
            uids = await self._get_followship('followers', did)
            await self.cache_db.executemany(
                'INSERT OR IGNORE INTO follow_graph (from_uid, to_uid) VALUES (?, ?)',
                [
                    (f, uid)
                    for f in uids
                ],
            )
            await self.cache_db.commit()

        if following > 0 and following < self.config.max_followers:
            uids = await self._get_followship('following', did)
            await self.cache_db.executemany(
                'INSERT OR IGNORE INTO follow_graph (from_uid, to_uid) VALUES (?, ?)',
                [
                    (uid, f)
                    for f in uids
                ],
            )
            await self.cache_db.commit()
        await self._mark_fetched(did, followers, following)
        return True

    async def ensure_graph(self, dids: list[str]):
        fetched: list[bool] = await tqdm_asyncio.gather(*[
            self._fetch_user_graph(did)
            for did in dids
        ])
        return [did for i, did in enumerate(dids) if not fetched[i]]

    async def all_edges(self) -> list[tuple[int, int]]:
        edges: list[tuple[int, int]] = []
        async with self.cache_db.execute(
            'SELECT from_uid, to_uid FROM follow_graph',
        ) as c:
            async for record in c:
                edges.append((record[0], record[1]))
        return edges

    async def all_users(self) -> dict[int, User]:
        users: dict[int, User] = {}
        async with self.cache_db.execute(
            'SELECT uid, did, handle, nick, desc FROM user',
        ) as c:
            async for record in c:
                uid, did, handle, nick, desc = record
                users[uid] = User(
                    did=did,
                    handle=handle,
                    nick=nick,
                    desc=desc,
                )
        return users

