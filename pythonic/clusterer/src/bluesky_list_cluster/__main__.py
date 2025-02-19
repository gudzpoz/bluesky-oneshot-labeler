import argparse
import asyncio
import logging

from . import BlueskyCluster, Config, RelDatabase
from .relationship import init_db


async def run():
    args = argparse.ArgumentParser()
    args.add_argument('-c', '--config', type=str, default='./config.json')
    args.add_argument('-d', '--debug', action='store_true')
    args = args.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    # Do not show httpx logs
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('aiosqlite').setLevel(logging.WARNING)

    config = Config.load(args.config)
    clustering = BlueskyCluster(await init_db(config), config)
    bad_uids = await clustering.update_all()
    new_blocks = await clustering.rank_all(set(bad_uids))
    clustering.add_blocks(new_blocks)
    await clustering.close()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
