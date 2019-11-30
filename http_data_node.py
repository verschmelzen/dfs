from urllib.request import urlopen
from urllib.parse import urlparse, urljoin

from util import CommandError


def deserialize_tuple(stream, content_len, remote_ip):
    tmp = stream.read(content_len).decode('utf-8')
    total, used, free = (int(x) for x in tmp.split())
    return total, used, free

def deserialize_list(stream, content_len, remote_ip):
    tmp = stream.read(content_len).decode('utf-8')
    return tmp.split()

def deserialize_stat(stream, content_len, remote_ip):
    tmp = stream.read(content_len).decode('utf-8')
    tmp = tmp.split()
    return tmp[0], int(tmp[1])


class HttpDataNode:

    def __init__(self, url):
        if not urlparse(url).netloc:
            raise CommandError(f'Invalid node url {url}')
        self._url = url

    def mkfs(self):
        urlopen(urljoin(self._url, '/mkfs')).close()

    def df(self) -> tuple:
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

    def ls(self, path: str = None) -> list:
        with urlopen(
            urljoin(self._url, '/ls'),
            data=path.encode('utf-8') if path else b'',
        ) as resp:
            return deserialize_list(resp)

    def mkdir(self, path: str):
        urlopen(
            urljoin(self._url, '/mkdir'),
            data=path.encode('utf-8'),
        ).close()

    def rmdir(self, path: str, force=False):
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

    def stat(self, path: str) -> tuple:
        with urlopen(
            urljoin(self._url, '/stat'),
            data=path.encode('utf-8'),
        ) as resp:
            return deserialize_stat(resp)

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

