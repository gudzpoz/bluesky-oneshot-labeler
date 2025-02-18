import logging
import typing

from sknetwork.data import Dataset, from_edge_list
from sknetwork.ranking import PageRank


from .blocklist import BlockList
from .config import Config
from .relationship import RelDatabase


_logger = logging.getLogger(__name__)
_debug = _logger.debug
_info = _logger.info
_warn = _logger.warning
# _logger.setLevel(logging.DEBUG)


class BlueskyCluster:
    config: Config
    db: RelDatabase

    def __init__(self, db: RelDatabase, config: Config) -> None:
        super().__init__()
        self.config = config
        self.db = db
        self.list = BlockList(config.output_csv)

    async def update_all(self):
        dids = list(self.list.bad_dids())
        uids = await self.db.ensure_users(dids)
        await self.db.ensure_graph(dids)
        return uids

    def rank_all(self, bad_uids: set[int], edge_list: list[tuple[int, int]]):
        graph = typing.cast(Dataset, from_edge_list(
            edge_list=edge_list,
            directed=False,
        ))
        adjacency = graph.adjacency
        dids = graph.names
        weights: dict[int, float] = {}
        for i, did in enumerate(dids):
            weights[i] = 1.0 if did in bad_uids else 0.1
        page_rank = PageRank(damping_factor=self.config.page_rank_damping)
        page_rank.fit(adjacency, weights=weights)
        scores = page_rank.predict()

        # indices = np.argsort(scores)[::-1]
        # scores = scores[indices]
        # dids = np.array(dids)[indices]
        # info = {
        #     row[0]: row[1:]
        #     for row in self.cache_db.execute('SELECT did, depth, handle, display_name, description FROM user')
        # }
        # df = pd.DataFrame(
        #     [(score, *info[did], did) for score, did in zip(scores, dids)],
        #     columns=['score', 'depth', 'handle', 'name', 'description', 'did'],
        # )
        # df.to_csv(self.config.output_csv, index=False)
        # return list(dids[np.logical_and(
        #     scores > self.config.rank_threshold,
        #     df['depth'] != 0,
        # )])

    async def close(self):
        await self.db.close()

