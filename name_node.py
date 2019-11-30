import random
from urllib.parse import urljoin

from util import *
from members import *
from http_data_node import HttpDataNode


class NameNode:

    @staticmethod
    def get_args(env):
        return (env['DFS_DB_PATH'],)

    def __init__(self, db_path):
        self._db = MemberDB(db_path)

    def add_node(self, url, node_id):
        # TODO: assign status NEW for handling heartbeat thread
        member = self._db.get(node_id)
        if member:
            raise CommandError(f'{node_id} is already a member')
        self._db.create(node_id, url, ALIVE)

    def status(self):
        return [(n.id, n.status) for n in self._db.filter()]

    def mkfs(self):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).mkfs()

    def df(self):
        nodes = self._db.filter(status=ALIVE)
        total = []
        for node in nodes:
            total.append(
                [node.id, *HttpDataNode(node.url).df()]
            )
        return total

    def cd(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).cd(path)

    def ls(self, path: str = None) -> list:
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.url, 'ls')

    def mkdir(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).mkdir(path)

    def rmdir(self, path: str, force=False):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).rmdir(path, force)

    def touch(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).touch(path)

    def cat(self, path: str) -> bytes:
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.url, 'cat')

    def tee(self, path: str, data: bytes):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).tee(path, data)

    def rm(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).rm(path)

    def stat(self, path: str) -> tuple:
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.url, 'stat')

    def cp(self, src: str, dst: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).cp(src, dst)

    def mv(self, src: str, dst: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).mv(src, dst)

    HANDLERS = {
        '/add_node': (add_node, deserialize, serialize),
        '/status': (status, deserialize, serialize_matrix),

        '/mkfs': (mkfs, deserialize, serialize),
        '/df': (df, deserialize, serialize_matrix),
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

        '/nodes/join': (add_node, deserialize_join, serialize),
    }

