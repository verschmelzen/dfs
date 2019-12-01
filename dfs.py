#!/usr/bin/env python
import logging
from errno import ENOENT
from stat import S_IFDIR
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

from http_name_node import HttpNameNode


class DFS(LoggingMixIn, Operations):

    def __init__(self, url):
        self._node = HttpNameNode(url)

    def create(self, path, mode):
        try:
            self._node.touch(path)
            return 0
        except:
            raise FuseOSError(ENOENT)

    def getattr(self, path, fh=None):
        try:
            st = self._node.stat(path)
            return {
                'st_size': st[1],
                'st_mode': st[2],
                'st_nlink': 1,
            }
        except:
            raise FuseOSError(ENOENT)

    def mkdir(self, path, mode):
        try:
            return self._node.mkdir(path)
        except:
            raise FuseOSError(ENOENT)

    def read(self, path, size, offset, fh):
        try:
            return self._node.cat(path)
        except:
            raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        try:
            return self._node.ls(path)
        except:
            raise FuseOSError(ENOENT)

    def readlink(self, path):
        try:
            return self._node.stat(path)[0]
        except:
            raise FuseOSError(ENOENT)

    def rename(self, old, new):
        try:
            return self._node.mv(old, new)
        except:
            raise FuseOSError(ENOENT)

    def rmdir(self, path):
        try:
            return self._node.rmdir(path)
        except:
            raise FuseOSError(ENOENT)

    def truncate(self, path, length, fh=None):
        try:
            return self._node.stat(path)[1]
        except:
            raise FuseOSError(ENOENT)

    def unlink(self, path):
        try:
            return self._node.rm(path)
        except:
            raise FuseOSError(ENOENT)

    def write(self, path, data, offset, fh):
        try:
            self._node.tee(path, data)
        except:
            raise FuseOSError(ENOENT)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='DFS mount tool'
    )
    parser.add_argument('mount', help='Mount directory')
    parser.add_argument('host', help='Name node hostname')
    parser.add_argument(
        'port',
        nargs='?',
        default='8180',
        help='Port to use for connection (default: 8180)',
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    url = f'http://{args.host}:{args.port}/'
    fuse = FUSE(
        DFS(url),
        args.mount,
        foreground=True,
        nothreads=True,
        allow_other=True,
    )

