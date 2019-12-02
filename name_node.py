import random
import time
import threading as th
from urllib.parse import urljoin

from util import *
from members import *
from http_data_node import HttpDataNode


def track_members(db, timeout, stop_event):
    while True:
        members = db.filter()
        for m in members:
            node = HttpDataNode(m.url)
            if m.status == NEW:
                node.mkfs()
                try:
                    donor = random.choice(db.filter(status=ALIVE))
                    node.sync(donor.url)
                    donor_node = HttpDataNode(donor.url)
                    node.cd(donor_node.stat('.')[0])
                except IndexError as e:
                    continue
                finally:
                    m.status = ALIVE
            else:
                if node.ping_alive():
                    if m.status == DEAD:
                        node.mkfs()
                        try:
                            donor = random.choice(db.filter(status=ALIVE))
                            node.sync(donor.url)
                            donor_node = HttpDataNode(donor.url)
                            node.cd(donor_node.stat('.')[0])
                        except IndexError as e:
                            continue
                        finally:
                            m.status = ALIVE
                else:
                    m.status = DEAD
        if stop_event.is_set():
            return
        time.sleep(timeout)


class NameNode:

    DEFAULT_HEARTBEAT = 1

    @staticmethod
    def get_args(env):
        return (env['DFS_DB_PATH'], env.get('DFS_HEARTBEAT'))

    def __init__(self, db_path, heartbeat=None):
        self._heartbeat_stop = None
        self._heartbeat = None
        self._db = MemberDB(db_path)
        heartbeat = heartbeat or self.DEFAULT_HEARTBEAT
        self._heartbeat_stop = th.Event()
        self._heartbeat = th.Thread(
            target=track_members,
            args=(self._db, heartbeat, self._heartbeat_stop),
        )
        self._heartbeat.start()

    def add_node(self, public_url, url, node_id):
        self._db.create(node_id, url, public_url=public_url)

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
        return urljoin(node.public_url, 'ls')

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
        return urljoin(node.public_url, 'cat')

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
        return urljoin(node.public_url, 'stat')

    def cp(self, src: str, dst: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).cp(src, dst)

    def mv(self, src: str, dst: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).mv(src, dst)

    def ping_alive(self):
        return True

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
        '/ping_alive': (ping_alive, deserialize, serialize),
    }

    def __del__(self):
        if self._heartbeat:
            self._heartbeat_stop.set()
            self._heartbeat.join()

