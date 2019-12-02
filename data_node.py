import os
import sys
import shutil
from typing import Optional, Tuple, List
from urllib.request import urlopen, URLError, HTTPError
from urllib.parse import urljoin, urlparse

from util import *


class DataNode:
    """
    Python API for direct interaction with filesystem. Isolates all
    operation in filesystem root directory, handles working directory
    path and absolute and relative path resolution Defines handlers
    for providing itself through HTTP API.

    Class Attributes
    ----------------
    HANDLERS : dict
        Dictionary where keys are HTTP endpoints and values are tuples
        of three elements: method to call, argument deserialization
        routine and return value serialization routine.
    """

    @staticmethod
    def get_args(env: os.environ) -> tuple:
        """
        Provides server with args to build an instance of DataNode.

        Parameters
        ----------
        env : os.environ
            Map of environment variables from server.

        Returns
        -------
        tuple:
            Tuple with arguments for constructor.
        """
        return (
            env['DFS_FS_ROOT'],
            env.get('DFS_NAMENODE_URL'),
            env.get('DFS_PORT', '8180'),
            env.get('DFS_ADVERTISE_HOST'),
            env.get('DFS_PUBLIC_URL'),
        )

    def __init__(
        self,
        fs_root: str,
        namenode_url: Optional[str] = None,
        port: Optional[str] = None,
        advertise_host: Optional[str] = None,
        public_url: Optional[str] = None,
    ):
        """
        Initialize filesystem state or recover from existing state
        on disk. Connect to cluster.

        Use namenode_url to connect to cluster and provide information
        for name node about possible connections to own server from
        user and name node itself.

        Parameters
        ----------
        fs_root : str
            Path to the root of virtual filesystem.
        namenode_url : Optional[str]
            Full url of cluster to connect to.
        port : Optional[str]
            Advertise port for users and name node to connect.
        advertise_host : Optional[str]
            Advertise host for users and name node to connect.
        public_url : Optional[str]
            Advertise url for cluster users to connect from behind NAT
            or ambiguous network.
        """
        self._fs_root = fs_root.rstrip('/')
        self._workdir = '/'
        self._state_file = self._fs_root + '.state'
        self._advertise_host = advertise_host
        self._public_url = public_url
        self._advertise_port = port
        self._namenode_url = None
        self._id = None
        if os.path.isfile(self._state_file):
            with open(self._state_file, 'r') as f:
                self._id, self._namenode_url = f.read().split()
                return
        self._id = gen_id()
        if namenode_url:
            if not self._advertise_port:
                raise ValueError(
                    'DFS_PORT should be set explicitly '
                    'when running in cluster mode'
                )
            self.join_namespace(namenode_url)
        with open(self._state_file, 'w') as f:
            f.write(self._id)
            f.write('\n')
            f.write(self._namenode_url)

    def _path_to_fs(self, path: str) -> str:
        return path_join(
            self._fs_root,
            path_join('/', self._workdir.strip('/'), path).strip('/')
        )

    def _fs_to_path(self, fs_path: str) -> str:
        return fs_path[len(self._fs_root):] or '/'

    def mkfs(self):
        """
        Create filesystem directory and force remove already directory
        if already exists. Reset working directory.
        """
        if os.path.exists(self._fs_root):
            shutil.rmtree(self._fs_root)
        os.makedirs(self._fs_root)
        self._workdir = '/'

    def df(self) -> Tuple[int, int, int]:
        """
        Return disk usage statistics about the given path.

        Returns
        -------
        Tuple[int, int, int]:
            Tuple with attributes with three elements which are
            the amount of total, used and free space, in bytes.
        """
        return shutil.disk_usage(self._fs_root)

    def cd(self, path: str):
        """
        Change working directory

        Parameters
        ----------
        path : str
            New workdir path, relative or absolute
        """
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if not os.path.isdir(fs_path):
            raise CommandError(f'{path} is not a dir')
        self._workdir = self._fs_to_path(fs_path)

    def ls(self, path: Optional[str] = None) -> List[str]:
        """
        List contents of directory.

        Parameters
        ----------
        path : Optional[str]
            Path to directory. If not specified current working
            directory is used.

        Returns
        -------
        List[str]:
            Array of directory entry names.
        """
        if path == None:
            path = ''
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if not os.path.isdir(fs_path):
            raise CommandError(f'{path} is not a dir')
        return os.listdir(fs_path)

    def mkdir(self, path: str):
        """
        Create new directory.

        Parameters
        ----------
        path : str
            Path to new directory.
        """
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise CommandError(f'{path} already exists')
        os.makedirs(fs_path)

    def rmdir(self, path: str, force: Optional[bool] = False):
        """
        Remove directory if it has no entries.

        Parameters
        ----------
        path : str
            Path to directory.
        force : Optional[bool]
            Remove directory even if it has entries. If directory has
            entries, but False is given error is raised.
            False by default.
        """
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
        """
        Create empty file if not exists.

        Parameters
        ----------
        path : str
            Path to file.
        """
        fs_path = self._path_to_fs(path)
        open(fs_path, 'a').close()

    def cat(self, path: str) -> bytes:
        """
        Read file contents.

        Parameters
        ----------
        path : str
            Path to file.

        Returns
        -------
        bytes:
            Contents of file.
        """
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        with open(fs_path, 'rb') as file:
            return file.read()

    def tee(self, path: str, data: bytes):
        """
        Write data to file. Remove all previous data.

        Parameters
        ----------
        path : str
            Path to file.
        data : bytes
            Data to write.
        """
        fs_path = self._path_to_fs(path)
        with open(fs_path, 'wb') as file:
            file.write(data)

    def rm(self, path: str):
        """
        Remove file.

        Parameters
        ----------
        path : str
            Path to file.
        """
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        if os.path.isdir(fs_path):
            raise CommandError(f'{path} is a directory')
        os.remove(fs_path)

    def stat(self, path: str) -> Tuple[str, int, int]:
        """
        Get info about file or directory: full path, size in bytes
        and mode.

        Parameters
        ----------
        path : str
            Path to file or directory.

        Returns
        -------
        Tuple[str, int, int]:
            Full path, size in bytes and mode.
        """
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise CommandError(f'{path} does not exist')
        st = os.stat(fs_path)
        return self._fs_to_path(fs_path), st.st_size, st.st_mode

    def cp(self, src: str, dst: str):
        """
        Copy file.

        Parameters
        ----------
        src : str
            Source file path.
        dst : str
            Path to write to.
        """
        abs_src = self._path_to_fs(src)
        abs_dst = self._path_to_fs(dst)
        if not os.path.exists(abs_src):
            raise CommandError(f'{src} does not exist')
        shutil.copy(abs_src, abs_dst)

    def mv(self, src: str, dst: str):
        """
        Move file.

        Parameters
        ----------
        src : str
            Source file path.
        dst : str
            New file path.
        """
        abs_src = self._path_to_fs(src)
        abs_dst = self._path_to_fs(dst)
        if not os.path.exists(abs_src):
            raise CommandError(f'{src} does not exist')
        shutil.move(abs_src, abs_dst)

    def sync(self, donor_url: str):
        """
        Synchronize filesystem state from donor data node.

        Parameters
        ----------
        donor_url : str
            URL to access donor node.
        """
        with urlopen(urljoin(donor_url, '/snap')) as resp:
            unpack(resp.read(), self._fs_root)

    def snap(self) -> bytes:
        """
        Create filesystem snapshot.

        Returns
        -------
        bytes:
            gzip compressed tarball of filesystem
        """
        return package(self._fs_root)

    def ping_alive(self) -> True:
        """
        Return True if server is alive.
        """
        return True

    def join_namespace(self, namenode_url: str):
        """
        Request join to cluster. Advertise to name node URLs to access
        own server from name node and from user.

        Parameters
        ----------
        namenode_url : str
            URL to name node server.
        """
        if self._namenode_url:
            raise CommandError(
                'Can only join one namespace. '
                f'Please leave {self._namenode_url} first'
            )
        if not urlparse(namenode_url).netloc:
            raise CommandError(f'Invalid namenode url {namenode_url}')
        data = self._advertise_port + ' ' + self._id
        if self._advertise_host:
            data = self._advertise_host + ':' + data
        if self._public_url:
            data = self._public_url + ' ' + data
        urlopen(
            urljoin(namenode_url, '/nodes/join'),
            data=data.encode('utf-8'),
        ).close()
        self._namenode_url = namenode_url

    def leave_namespace(self):
        """
        Exit cluster.
        """
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
        '/ping_alive': (ping_alive, deserialize, serialize),

        '/join_namespace': (join_namespace, deserialize, serialize),
        '/leave_namespace': (leave_namespace, deserialize, serialize),
    }

