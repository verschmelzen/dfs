from typing import Optional, List, Tuple
from urllib.request import urlopen, HTTPError, URLError
from urllib.parse import urlparse, urljoin

from util import *


class HttpNameNode:
    """
    Python API for client of remote DFS cluster. It implements
    same API as NameNode, but delegates actual execution to remote
    name node server instance.
    """

    def __init__(self, url: str):
        """
        Set name node server url.

        Parameters
        ----------
        url : str
            URL to data node server.
        """
        if not urlparse(url).netloc:
            raise CommandError(f'Invalid node url {url}')
        self._url = url

    def add_node(self, public_url: str, url: str, node_id: str):
        data = url + ' ' + node_id
        urlopen(
            urljoin(self._url, '/add_node'),
            data=data.encode('utf-8'),
        ).close()

    def status(self) -> List[Tuple[str, int]]:
        with urlopen(urljoin(self._url, '/status')) as resp:
            return deserialize_matrix(
                resp,
                resp.length,
                urlparse(resp.url).hostname,
            )

    def mkfs(self):
        urlopen(urljoin(self._url, '/mkfs')).close()

    def df(self) -> List[Tuple[str, int, int, int]]:
        with urlopen(urljoin(self._url, '/df')) as resp:
            return deserialize_matrix(
                resp,
                resp.length,
                urlparse(resp.url).hostname,
            )

    def cd(self, path: str):
        urlopen(
            urljoin(self._url, '/cd'),
            data=path.encode('utf-8'),
        ).close()

    def ls(self, path: Optional[str] = None) -> list:
        data = path.encode('utf-8') if path else b''
        with urlopen(
            urljoin(self._url, '/ls'),
            data=data,
        ) as resp:
            url = resp.read().decode('utf-8')
        with urlopen(
            url,
            data=data,
        ) as resp:
            return deserialize_list(
                resp,
                resp.length,
                urlparse(resp.url).hostname,
            )

    def mkdir(self, path: str):
        urlopen(
            urljoin(self._url, '/mkdir'),
            data=path.encode('utf-8'),
        ).close()

    def rmdir(self, path: str, force: Optional[bool] = False):
        data = path + ('!' if force else '')
        urlopen(
            urljoin(self._url, '/rmdir'),
            data=data.encode('utf-8'),
        ).close()

    def touch(self, path: str):
        urlopen(
            urljoin(self._url, '/touch'),
            data=path.encode('utf-8'),
        ).close()

    def cat(self, path: str) -> bytes:
        data = path.encode('utf-8')
        with urlopen(
            urljoin(self._url, '/cat'),
            data=data,
        ) as resp:
            url = resp.read().decode('utf-8')
        with urlopen(
            url,
            data=data,
        ) as resp:
            return resp.read()

    def tee(self, path: str, data: bytes):
        data = path.encode('utf-8') + b'\0' + data
        urlopen(
            urljoin(self._url, '/tee'),
            data=data,
        ).close()

    def rm(self, path: str):
        urlopen(
            urljoin(self._url, '/rm'),
            data=path.encode('utf-8'),
        ).close()

    def stat(self, path: str) -> tuple:
        data = path.encode('utf-8')
        with urlopen(
            urljoin(self._url, '/stat'),
            data=data,
        ) as resp:
            url = resp.read().decode('utf-8')
        with urlopen(
            url,
            data=data,
        ) as resp:
            return deserialize_stat(
                resp,
                resp.length,
                urlparse(resp.url).hostname,
            )

    def cp(self, src: str, dst: str):
        data = src + ' ' + dst
        urlopen(
            urljoin(self._url, '/cp'),
            data=data.encode('utf-8'),
        ).close()

    def mv(self, src: str, dst: str):
        data = src + ' ' + dst
        urlopen(
            urljoin(self._url, '/mv'),
            data=data.encode('utf-8'),
        ).close()

    def ping_alive(self) -> bool:
        try:
            urlopen(urljoin(self._url, '/ping_alive')).close()
            return True
        except (HTTPError, URLError):
            return False

