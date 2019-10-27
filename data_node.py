import os
import shutil


def path_join(first, *args):
    return os.path.normpath(
        os.path.join(first, *(a.strip('/') for a in args))
    )


class DataNode:

    def __init__(self, fs_root: str):
        self._fs_root = fs_root.rstrip('/')
        self._workdir = '/'

    def _path_to_fs(self, path: str) -> str:
        if path.startswith('/'):
            return path_join(self._fs_root, path)
        return path_join(self._fs_root, self._workdir, path)

    def _fs_to_path(self, fs_path):
        return fs_path[len(self._fs_root):] or '/'

    def mkfs(self):
        if os.path.exists(self._fs_root):
            shutil.rmtree(self._fs_root)
        os.makedirs(self._fs_root)

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
        if not os.path.isdir(fs_path):
            raise Exception(f'{path} is not a dir')
        self._workdir = self._fs_to_path(fs_path)

    def ls(self, path: str = None) -> list:
        if path == None:
            path = self._workdir
        fs_path = self._path_to_fs(path)
        if not os.path.isdir(fs_path):
            raise Exception(f'{path} is not a dir')
        return os.listdir(fs_path)

    def mkdir(self, path: str):
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise Exception(f'{path} already exists')
        os.makedirs(fs_path)

    def rmdir(self, path: str, force=False):
        fs_path = self._path_to_fs(path)
        if not os.path.isdir(fs_path):
            raise Exception(f'{path} is not a dir')
        if len(os.listdir(path)) > 0 and not force:
            raise Exception(f'{path} is not empty')
        shutil.rmtree(fs_path)

    def touch(self, path: str):
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise Exception(f'{path} already exists')
        open(fs_path, 'a').close()

    def cat(self, path: str) -> bytes:
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise Exception(f'{path} does not exist')
        with open(fs_path, 'rb') as file:
            return file.read()

    def tee(self, path: str, data: bytes):
        fs_path = self._path_to_fs(path)
        if os.path.exists(fs_path):
            raise Exception(f'{path} already exist')
        with open(fs_path, 'wb') as file:
            file.write(data)

    def rm(self, path: str):
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise Exception(f'{path} does not exist')
        if os.path.isdir(fs_path):
            raise Exception(f'{path} is a directory')
        os.remove(fs_path)

    def stat(self, path: str) -> tuple:
        fs_path = self._path_to_fs(path)
        if not os.path.exists(fs_path):
            raise Exception(f'{path} does not exist')
        return self._fs_to_path(fs_path), os.stat(fs_path).st_size

    def cp(self, src: str, dst: str):
        abs_src = self._path_to_fs(src)
        abs_dst = self._path_to_fs(dst)
        if not os.path.exists(abs_src):
            raise Exception(f'{src} does not exist')
        if os.path.exists(abs_dst):
            raise Exception(f'{dst} already exists')
        shutil.copy(abs_src, abs_dst)

    def mv(self, src: str, dst: str):
        abs_src = self._path_to_fs(src)
        abs_dst = self._path_to_fs(dst)
        if not os.path.exists(abs_src):
            raise Exception(f'{src} does not exist')
        if os.path.exists(abs_dst):
            raise Exception(f'{dst} already exists')
        shutil.move(abs_src, abs_dst)

    def _deserialize(self, stream):
        has_blob = False
        path = b''
        b = stream.read(1)
        while b:
            if b == b' ':
                break
            if b == b'\0':
                has_blob = True
                break
            path += path + b
            b = stream.read(1)
        path = path.decode('utf-8')
        if not b:
            return path

        if has_blob:
            return path, stream.read()

        b = stream.read(1)
        if b == b'!':
            nb = stream.read(1)
            if not nb:
                return path, True
            b += nb
        b += stream.read()
        return path, b.decode('utf-8')

    URLS = {
        '/mkfs': (mkfs, _deserialize),
        '/cd': (cd, _deserialize),
        '/ls': (ls, _deserialize),
        '/mkdir': (mkdir, _deserialize),
        '/rmdir': (rmdir, _deserialize),
        '/touch': (touch, _deserialize),
        '/cat': (cat, _deserialize),
        '/rm': (rm, _deserialize),
        '/stat': (stat, _deserialize),
        '/cp': (cp, _deserialize),
        '/mv': (mv, _deserialize),
    }

