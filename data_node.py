import os
import shutil


class DataNode:

    def __init__(self, root: str):
        self.root = root
        self.workdir = root

    def mkfs(self):
        pass

    def _resolve_path(self, path: str) -> str:
        if path.startswith('/'):
            return self.root + path
        return os.path.join(self.workdir, path)

    def _dfs_path(self, abs_path):
        return abs_path[len(self.root) + 1:]

    def cd(self, path: str):
        abs_path = self._resolve_path(path)
        if not os.path.isdir(abs_path):
            raise Exception(f'{path} is not a dir')
        self.workdir = abs_path

    def ls(self, path: str) -> list:
        abs_path = self._resolve_path(path)
        if not os.path.isdir(abs_path):
            raise Exception(f'{path} is not a dir')
        return os.listdir(abs_path)

    def mkdir(self, path: str):
        abs_path = self._resolve_path(path)
        if os.path.exists(abs_path):
            raise Exception(f'{path} already exists')
        os.makedirs(abs_path)

    def rmdir(self, path: str, force=False):
        abs_path = self._resolve_path(path)
        if not os.path.isdir(abs_path):
            raise Exception(f'{path} is not a dir')
        if len(os.listdir(path)) > 0 and not force:
            raise Exception(f'{path} is not empty')
        shutil.rmtree(abs_path)

    def touch(self, path: str):
        abs_path = self._resolve_path(path)
        if os.path.exists(abs_path):
            raise Exception(f'{path} already exists')
        open(abs_path, 'a').close()

    def cat(self, path: str) -> bytes:
        abs_path = self._resolve_path(path)
        if not os.path.exists(abs_path):
            raise Exception(f'{path} does not exist')
        with open(abs_path, 'rb') as file:
            return file.read()

    def tee(self, path: str, data: bytes):
        abs_path = self._resolve_path(path)
        if os.path.exists(abs_path):
            raise Exception(f'{path} already exist')
        with open(abs_path, 'wb') as file:
            file.write(data)

    def rm(self, path: str):
        abs_path = self._resolve_path(path)
        if not os.path.exists(abs_path):
            raise Exception(f'{path} does not exist')
        if os.path.isdir(abs_path):
            raise Exception(f'{path} is a directory')
        os.remove(abs_path)

    def stat(self, path: str) -> tuple:
        abs_path = self._resolve_path(path)
        if not os.path.exists(abs_path):
            raise Exception(f'{path} does not exist')
        if not os.path.isfile(abs_path):
            raise Exception(f'{path} is not a file')
        return self._dfs_path(abs_path), os.stat(abs_path).st_size

    def cp(self, src: str, dst: str):
        abs_src = self._resolve_path(src)
        abs_dst = self._resolve_path(dst)
        if not os.path.exists(abs_src):
            raise Exception(f'{src} does not exist')
        if os.path.exists(abs_dst):
            raise Exception(f'{dst} already exists')
        shutil.copy(abs_src, abs_dst)

    def mv(self, src: str, dst: str):
        abs_src = self._resolve_path(src)
        abs_dst = self._resolve_path(dst)
        if not os.path.exists(abs_src):
            raise Exception(f'{src} does not exist')
        if os.path.exists(abs_dst):
            raise Exception(f'{dst} already exists')
        shutil.move(abs_src, abs_dst)
