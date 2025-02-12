import argparse
import logging

from . import BlueskyListCluster, Config


def main():
    args = argparse.ArgumentParser()
    args.add_argument('list_uri', nargs='+')
    args.add_argument('-c', '--config', type=str, default='./config.json')
    args.add_argument('-u', '--update', action='store_true')
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
        dids = clustering.rank_all()
        if args.update:
            clustering.add_to_list(dids)
        else:
            logging.info('List candidates: %d', len(dids))
    finally:
        clustering.close()


if __name__ == "__main__":
    main()
