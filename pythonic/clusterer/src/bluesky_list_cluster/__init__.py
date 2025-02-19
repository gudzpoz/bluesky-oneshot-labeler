import logging
import typing

import numpy as np
import pandas as pd
from scipy import sparse
from sknetwork.data import from_edge_list
from sknetwork.ranking import PageRank


from .blocklist import BlockList
from .config import Config
from .relationship import RelDatabase


_logger = logging.getLogger(__name__)
_debug = _logger.debug
_info = _logger.info
_warn = _logger.warning


class BlueskyCluster:
    config: Config
    db: RelDatabase
    blist: BlockList

    def __init__(self, db: RelDatabase, config: Config) -> None:
        super().__init__()
        self.config = config
        self.db = db
        self.blist = BlockList(config.blocked_csv)

    async def update_all(self):
        dids = list(self.blist.bad_dids())
        uids = await self.db.ensure_users(dids)
        _debug('bad dids: %d -> resolved uids: %d', len(dids), len(uids))
        users_not_found = await self.db.ensure_graph(dids)
        msg = '(account removed)'
        for did in users_not_found:
            u = self.blist[did]
            if msg not in u.reason:
                self.blist[did].reason = f'{msg}{u.reason}'
        return uids

    async def rank_all(self, bad_uids: set[int]):
        edge_list = await self.db.all_edges()
        adjacency = typing.cast(sparse.csr_matrix, from_edge_list(
            edge_list=edge_list,
            directed=True,
        ))
        uids = set(uid for uid, _ in edge_list) | set(uid for _, uid in edge_list)
        weights: dict[int, float] = {}
        for i, uid in enumerate(uids):
            weights[i] = 1.0 if uid in bad_uids else 0.1
        page_rank = PageRank(damping_factor=self.config.page_rank_damping)
        page_rank.fit(adjacency, weights=weights)
        scores = page_rank.predict()

        indices = np.argsort(scores)[::-1]
        info = await self.db.all_users()
        lines = []
        for uid in indices:
            if uid not in uids:
                continue
            score = scores[uid]
            u = info[uid]
            lines.append((
                score,
                'y' if uid in bad_uids else 'n',
                u.nick,
                u.desc.replace('\n', ' '),
                u.handle,
                u.did,
            ))
        df = pd.DataFrame(
            lines,
            columns=['score', 'blocked', 'nick', 'description', 'handle', 'did'],
        )
        df.to_csv(self.config.output_csv, index=False)

        new_blocks = []
        for uid in np.where(scores > self.config.rank_threshold)[0]:
            if uid in bad_uids or uid not in uids:
                continue
            u = info[uid]
            new_blocks.append(u.did)
        _info('new blocks: %d', len(new_blocks))
        return new_blocks

    def add_blocks(self, new_blocks: list[str]):
        for did in new_blocks:
            self.blist.add(did, '', '')
        self.blist.write()

    async def close(self):
        await self.db.close()

