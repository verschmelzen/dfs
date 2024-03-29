from typing import Optional, List, Tuple
from urllib.request import urlopen, HTTPError, URLError
from urllib.parse import urlparse, urljoin

from util import *


class HttpDataNode:
    """
    Python API for interaction with remote DataNode server through HTTP.
    It implements same api as DataNode, but delegates actual execution
    to remote data node server through HTTP.
    """

    def __init__(self, url: str):
        """
        Set data node server url.

        Parameters
        ----------
        url : str
            URL to data node server.
        """
        if not urlparse(url).netloc:
            raise CommandError(f'Invalid node url {url}')
        self._url = url

    def mkfs(self):
        urlopen(urljoin(self._url, '/mkfs')).close()

    def df(self) -> Tuple[int, int, int]:
        with urlopen(urljoin(self._url, '/df')) as resp:
            return deserialize_tuple(
                resp,
                resp.length,
                urlparse(resp.url).hostname,
            )

    def cd(self, path: str):
        urlopen(
            urljoin(self._url, '/cd'),
            data=path.encode('utf-8'),
        ).close()

    def ls(self, path: Optional[str] = None) -> List[str]:
        with urlopen(
            urljoin(self._url, '/ls'),
            data=path.encode('utf-8') if path else b'',
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
        with urlopen(
            urljoin(self._url, '/cat'),
            data=path.encode('utf-8'),
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

    def stat(self, path: str) -> Tuple[str, int, int]:
        with urlopen(
            urljoin(self._url, '/stat'),
            data=path.encode('utf-8'),
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

    def sync(self, donor_url: str):
        urlopen(
            urljoin(self._url, '/sync'),
            data=donor_url.encode('utf-8'),
        ).close()

    def snap(self) -> bytes:
        with urlopen(urljoin(self._url, '/snap')) as resp:
            return resp.read()

    def ping_alive(self) -> bool:
        try:
            urlopen(urljoin(self._url, '/ping_alive')).close()
            return True
        except (HTTPError, URLError):
            return False

    def join_namespace(self, namenode_url: str):
        urlopen(
            urljoin(self._url, '/join_namespace'),
            data=namenode_url.encode('utf-8'),
        ).close()

    def leave_namespace(self):
        urlopen(urljoin(self._url, '/leave_namespace')).close()

