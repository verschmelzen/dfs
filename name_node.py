import random
import time
import threading as th
from typing import Optional, List, Tuple
from urllib.parse import urljoin

from util import *
from members import *
from http_data_node import HttpDataNode


def track_members(db: MemberDB, interval: int, stop_event: th.Event):
    """
    Main function of heartbeat thread of NameNode.

    It walks through database, initializes new nodes, checks if nodes
    are alive and synchronizes nodes that have been dead but came back.

    Parameters
    ----------
    db : MemberDb
        Database instance.
    interval : int
        Interval of checks.
    stop_event : threading.Event
        Notification of main thread cleanup and exit.
    """
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
        time.sleep(interval)


class NameNode:
    """
    Python API for DFS cluster operation. Handles data nodes in
    separate thread. It provides same interface as DataNode both in
    python and HTTP, and additional routines for cluster management.

    Class Attributes
    ----------------
    DEFAULT_HEARTBEAT : int
        Default interval of data node status checks.
    HANDLERS : dict
        Dictionary where keys are HTTP endpoints and values are tuples
        of three elements: method to call, argument deserialization
        routine and return value serialization routine.
    """

    DEFAULT_HEARTBEAT = 1

    @staticmethod
    def get_args(env: os.environ) -> tuple:
        """
        Provides server with args to build an instance of NameNode.

        Parameters
        ----------
        env : os.environ
            Map of environment variables from server.

        Returns
        -------
        tuple:
            Tuple with arguments for constructor.
        """
        return (env['DFS_DB_PATH'], env.get('DFS_HEARTBEAT'))

    def __init__(self, db_path: str, heartbeat: Optional[int] = None):
        """
        Initialize member database. Start heartbeat thread.

        Parameters
        ----------
        db_path : str
            Path to database file.
        heartbeat : Optional[int]
            Interval of member status checks. If not specified uses
            DEFAULT_HEARTBEAT value.
        """
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

    def add_node(self, public_url: str, url: str, node_id: str):
        """
        Add data node to members database with status NEW.

        Parameters
        ----------
        public_url : str
            Redirection URL user read operations.
        url : str
            URL for direct data node interaction.
        node_id : str
            Unique identifier of data node.
        """
        self._db.create(node_id, url, public_url=public_url)

    def status(self) -> List[Tuple[str, str]]:
        """
        Get status of data nodes.

        Returns
        -------
        List[Tuple[str, str]]:
            Array of pairs with data node id and its status.
        """
        return [(n.id, n.status) for n in self._db.filter()]

    def mkfs(self):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).mkfs()

    def df(self) -> List[Tuple[str, int, int, int]]:
        """
        Get disk usage info for members.

        Returns
        -------
        List[Tuple[str, int, int, int]]:
            Array of node id, total, used, and free bytes.
        """
        nodes = self._db.filter(status=ALIVE)
        total = []
        for node in nodes:
            total.append(
                (node.id, *HttpDataNode(node.url).df())
            )
        return total

    def cd(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).cd(path)

    def ls(self, path: Optional[str] = None) -> str:
        """
        Get endpoint for reading directory.

        Selects healthy data node and redirects user to read actual
        results from there.

        Parameters
        ----------
        path : Optional[str]
            Path to directory.

        Returns
        -------
        str:
            URL to action on healthy data node.
        """
        node = random.choice(self._db.filter(status=ALIVE))
        return urljoin(node.public_url, 'ls')

    def mkdir(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).mkdir(path)

    def rmdir(self, path: str, force: Optional[bool] = False):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).rmdir(path, force)

    def touch(self, path: str):
        nodes = self._db.filter(status=ALIVE)
        for node in nodes:
            HttpDataNode(node.url).touch(path)

    def cat(self, path: str) -> str:
        """
        Get endpoint for reading file.

        Selects healthy data node and redirects user to read actual
        results from there.

        Parameters
        ----------
        path : str
            Path to file.

        Returns
        -------
        str:
            URL to action on healthy data node.
        """
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

    def stat(self, path: str) -> str:
        """
        Get endpoint to fetch file or directory info.

        Selects healthy data node and redirects user to read actual
        results from there.

        Parameters
        ----------
        path : str
            Path to file or directory.

        Returns
        -------
        str:
            URL to action on healthy data node.
        """
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

