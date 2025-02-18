import argparse
import asyncio
import logging

from . import BlueskyCluster, Config, RelDatabase
from .relationship import init_db


async def run():
    args = argparse.ArgumentParser()
    args.add_argument('-c', '--config', type=str, default='./config.json')
    args = args.parse_args()

    logging.basicConfig(level=logging.INFO)
    # Do not show httpx logs
    logging.getLogger('httpx').setLevel(logging.WARNING)

    config = Config.load(args.config)
    clustering = BlueskyCluster(await init_db(config), config)
    await clustering.update_all()
    await clustering.close()


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
