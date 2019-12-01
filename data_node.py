import os
import sys
import shutil
from urllib.request import urlopen, URLError, HTTPError
from urllib.parse import urljoin, urlparse

from util import *


def is_subpath(base, path):
    return os.path.abspath(path).startswith(os.path.abspath(base))


class DataNode:

    @staticmethod
    def get_args(env):
        return (
            env['DFS_FS_ROOT'],
            env.get('DFS_NAMENODE_URL'),
            env.get('DFS_PORT', '8180'),
        )

    def __init__(self, fs_root: str, namenode_url=None, port=None):
        self._id = gen_id()
        self._fs_root = fs_root.rstrip('/')
        self._namenode_url = None
        self._advertise_port = port
        if namenode_url:
            if not self._advertise_port:
                raise ValueError(
                    'DFS_PORT should be set explicitly '
                    'when running in cluster mode'
                )
            self.join_namespace(namenode_url)
        self._workdir = '/'

    def _path_to_fs(self, path: str) -> str:
        return path_join(
            self._fs_root,
            path_join('/', self._workdir.strip('/'), path).strip('/')
        )

    def _fs_to_path(self, fs_path):
        return fs_path[len(self._fs_root):] or '/'

    def mkfs(self):
        if os.path.exists(self._fs_root):
            shutil.rmtree(self._fs_root)
        os.makedirs(self._fs_root)
        self._workdir = '/'

    def df(self) -> tuple:
        """
        Returns storage stats

        Returns
        -------
        tuple(int, int, int)
            total, used, free size in bytes on disk
        """
        return shutil.disk_usage(self._fs_root)

    def cd(self, path: str):
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if not os.path.isdir(fs_path):
            raise CommandError(f'{path} is not a dir')
        self._workdir = self._fs_to_path(fs_path)

    def ls(self, path: str = None) -> list:
        if path == None:
            path = ''
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if not os.path.isdir(fs_path):
            raise CommandError(f'{path} is not a dir')
        return os.listdir(fs_path)

    def mkdir(self, path: str):
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise CommandError(f'{path} already exists')
        os.makedirs(fs_path)

    def rmdir(self, path: str, force=False):
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if not os.path.isdir(fs_path):
            raise CommandError(f'{path} is not a dir')
        if fs_path == self._fs_root:
            raise CommandError(f'Cannot remove root dir')
        if len(os.listdir(fs_path)) > 0 and not force:
            raise CommandError(f'{path} is not empty')
        shutil.rmtree(fs_path)

    def touch(self, path: str):
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise CommandError(f'{path} already exists')
        open(fs_path, 'a').close()

    def cat(self, path: str) -> bytes:
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        with open(fs_path, 'rb') as file:
            return file.read()

    def tee(self, path: str, data: bytes):
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise CommandError(f'{path} already exist')
        with open(fs_path, 'wb') as file:
            file.write(data)

    def rm(self, path: str):
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if os.path.isdir(fs_path):
            raise CommandError(f'{path} is a directory')
        os.remove(fs_path)

    def stat(self, path: str) -> tuple:
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        return self._fs_to_path(fs_path), os.stat(fs_path).st_size

    def cp(self, src: str, dst: str):
        abs_src = self._path_to_fs(src)
        abs_dst = self._path_to_fs(dst)
        if not os.path.exists(abs_src):
            raise CommandError(f'{src} does not exist')
        if os.path.exists(abs_dst):
            raise CommandError(f'{dst} already exists')
        shutil.copy(abs_src, abs_dst)

    def mv(self, src: str, dst: str):
        abs_src = self._path_to_fs(src)
        abs_dst = self._path_to_fs(dst)
        if not os.path.exists(abs_src):
            raise CommandError(f'{src} does not exist')
        if os.path.exists(abs_dst):
            raise CommandError(f'{dst} already exists')
        shutil.move(abs_src, abs_dst)

    def sync(self, donor_url):
        with urlopen(urljoin(donor_url, '/snap')) as resp:
            unpack(resp.read(), self._fs_root)

    def snap(self):
        return package(self._fs_root)

    HANDLERS = {
        '/mkfs': (mkfs, deserialize, serialize),
        '/df': (df, deserialize, serialize),
        '/cd': (cd, deserialize, serialize),
        '/ls': (ls, deserialize, serialize),
        '/mkdir': (mkdir, deserialize, serialize),
        '/rmdir': (rmdir, deserialize, serialize),
        '/touch': (touch, deserialize, serialize),
        '/cat': (cat, deserialize, serialize),
        '/tee': (tee, deserialize, serialize),
        '/rm': (rm, deserialize, serialize),
        '/stat': (stat, deserialize, serialize),
        '/cp': (cp, deserialize, serialize),
        '/mv': (mv, deserialize, serialize),

        '/sync': (sync, deserialize, serialize),
        '/snap': (snap, deserialize, serialize),
    }

    def join_namespace(self, namenode_url):
        if self._namenode_url:
            raise CommandError(
                'Can only join one namespace. '
                f'Please leave {self._namenode_url} first'
            )
        if not urlparse(namenode_url).netloc:
            raise CommandError(f'Invalid namenode url {namenode_url}')
        data = self._advertise_port + ' ' + self._id
        urlopen(
            urljoin(namenode_url, '/nodes/join'),
            data=data.encode('utf-8'),
        ).close()
        self._namenode_url = namenode_url

    def leave_namespace(self):
        if self._namenode_url:
            raise CommandError('Not a member of namespace')
        try:
            urlopen(
                urljoin(self._namenode_url, '/nodes/leave')
            ).close()
        except (URLError, HTTPError):
            print(
                "Failed to notify namenode. Still leaving!",
                file=sys.stderr,
            )
        self._namenode_url = None

