import argparse
import logging

from . import BlueskyListCluster, Config


def main():
    args = argparse.ArgumentParser()
    args.add_argument('list_uri', nargs='+')
    args.add_argument('-c', '--config', type=str, default='./config.json')
    args = args.parse_args()

    logging.basicConfig(level=logging.INFO)
    # Do not show httpx logs
    logging.getLogger('httpx').setLevel(logging.WARNING)

    clustering = BlueskyListCluster(
        args.config,
        args.list_uri,
    )
    try:
        clustering.update_all()
    finally:
        clustering.close()


if __name__ == "__main__":
    main()
