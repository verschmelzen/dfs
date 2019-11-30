import random
from urllib.parse import urljoin

from util import (
    CommandError,
    path_join,
    import_class,
    deserialize,
    serialize,
)
from members import *


def deserialize_join(stream, content_len, remote_ip):
    return stream.read(content_len).decode('utf-8'), remote_ip


class NameNode:
    @staticmethod
    def get_args(env):
        return (
            env['DFS_DB_PATH'],
            env['DFS_MEMBER_CLS'],
            env['DFS_REPLICAS'],
        )

    def __init__(self, db_path, member_cls, replicas):
        self._nodes_file = path_join(self._db_path, 'nodes')
        self._member_cls = import_class(member_cls)
        self._replicas = replicas
        self._db = MemberDB(db_path)


    def list_nodes(self):
        return self._db.filter()

    def is_member(self, node_id):
        with _nodes_reader(self._nodes_file) as reader:
            try:
                next(x for x in reader if x[0] == node_id)
                return True
            except StopIteration:
                return False

    def add_node(self, url, node_id):
        if self.is_member(node_id):
            raise CommandError(f'{node_id} is already a member')
        self._member_cls(url).mkfs()
        with _nodes_writer(self._nodes_file, append=True) as writer:
            writer.writerow([node_id, url, HEALTHY])

    def exclude_node(self, node_id):
        with _nodes_reader(self._nodes_file) as reader:
            records = list(reader)
        new_records = [x for x in records if not x[0] == node_id]
        if len(records) == len(new_records):
            raise CommandError(f'{node_id} is not a member')
        with _nodes_writer(self._nodes_file) as writer:
            for r in new_records:
                writer.write(r)


    def mkfs(self):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).mkfs()

    def df(self):
        nodes = self._db.filter(status=ALIVE)
        total = []
        for node in nodes:
            total.append(
                [node.id, *self._member_cls(node.url).df()]
            )
        return total

    def cd(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).cd(path)

    def ls(self, path: str = None) -> list:
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.url, '/ls')

    def mkdir(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).mkdir(path)

    def rmdir(self, path: str, force=False):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).rmdir(path, force)

    def touch(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).touch(path)

    def cat(self, path: str) -> bytes:
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.url, '/cat')

    def tee(self, path: str, data: bytes):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).tee(path, data)

    def rm(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).rm(path)

    def stat(self, path: str) -> tuple:
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.url, '/stat')

    def cp(self, src: str, dst: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).cp(src, dst)

    def mv(self, src: str, dst: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            self._member_cls(node.url).mv(src, dst)
