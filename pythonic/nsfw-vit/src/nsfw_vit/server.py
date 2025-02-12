import json
import logging
import signal
import typing
import urllib.parse
from http import HTTPStatus
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor

import requests
from PIL import Image

from nsfw_vit import NsfwVitDetector, NsfwResult


_logger = logging.getLogger(__name__)
_debug = _logger.debug
_info = _logger.info


class NsfwError(typing.TypedDict):
    error: str


class NsfwVitDetectorHandler(BaseHTTPRequestHandler):
    def __init__(self, request, client_address, server, context: 'NsfwVitDetectorServer'):
        self.context = context
        super().__init__(request, client_address, server)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            content = urllib.parse.unquote(self.rfile.read(content_length).decode('utf-8'))
            image_urls = content.splitlines()
            _debug('(%s) image urls: %s', self, image_urls)
            images = self.context.fetch_images(images=image_urls)
            results = self.context.detect(images)

            self.send_response(HTTPStatus.OK)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            data = json.dumps(results)
            self.wfile.write(data.encode('utf-8'))
        except Exception as e:
            _debug('error: %s', e)
            self.send_response(HTTPStatus.BAD_REQUEST)
            self.end_headers()

class NsfwVitDetectorServer:
    def __init__(self, detector: NsfwVitDetector, port: int, timeout_ms: int):
        self.detector = detector
        self.timeout_ms = timeout_ms
        self.server = ThreadingHTTPServer(('', port), self.new_handler)
        self.workers = ThreadPoolExecutor(max_workers=32)
        signal.signal(
            signal.SIGINT,
            lambda _signum, _frame: self.workers.submit(self.shutdown),
        )

    def shutdown(self):
        _info('shutting down')
        self.server.shutdown()
        self.workers.shutdown(wait=False)

    def new_handler(self, request, client_address, server):
        return NsfwVitDetectorHandler(request, client_address, server, self)

    def _fetch_image(self, image: str) -> Image.Image | Exception:
        try:
            url = urllib.parse.urlparse(image)
            if url.scheme != 'https':
                return ValueError('expecting https:// scheme')
            response = requests.get(image, stream=True)
            return Image.open(response.raw) # type: ignore
        except Exception as e:
            return e

    def fetch_images(self, images: list[str]) -> list[Image.Image | Exception]:
        return list(self.workers.map(self._fetch_image, images, timeout=self.timeout_ms / 1000))

    def detect(self, images: list[Image.Image | Exception]):
        filtered = [image for image in images if isinstance(image, Image.Image)]
        filtered_results = [] if len(filtered) == 0 else self.detector.detect(filtered)
        results: list[NsfwResult | NsfwError] = []
        result_iter = iter(filtered_results)
        for image in images:
            if isinstance(image, Image.Image):
                results.append(next(result_iter))
            else:
                results.append(NsfwError(error=f'{image}'))
        return results

    def serve(self):
        _info('serving on http://localhost:%d', self.server.server_port)
        self.server.serve_forever()
