import argparse
import logging
import typing

import requests
from PIL import Image

from nsfw_vit import NsfwVitDetector
from nsfw_vit.server import NsfwVitDetectorServer

_logger = logging.getLogger(__name__)
_info = _logger.info

def preload(_: argparse.Namespace):
    detector = NsfwVitDetector()
    _info("model preloaded: %s", detector.model.config.name_or_path)

def serve(args: argparse.Namespace):
    detector = NsfwVitDetector()
    server = NsfwVitDetectorServer(detector, args.port, args.timeout_ms)
    server.serve()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    commands = parser.add_subparsers(help="preload model", required=True)
    commands.add_parser("preload", help="preload model").set_defaults(func=preload)
    server = commands.add_parser("serve", help="serve model")
    server.add_argument("-p", "--port", type=int, default=5000)
    server.add_argument("-t", "--timeout-ms", type=int, default=5000)
    server.set_defaults(func=serve)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    args.func(args)


if __name__ == "__main__":
    main()
