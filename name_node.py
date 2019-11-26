import csv
import multiprocessing as mp
from pathlib import Path
from contextlib import contextmanager
from importlib import import_module

from util import (
    CommandError,
    path_join,
    import_class,
    deserialize,
    serialize,
)


HEALTHY = 'healty'
DEAD = 'dead'


class Member:

    def __init__(self, id, url, status, database):
        self._id = id
        self._url = url
        self._status = status
        self._db = database

    def save(self):
        self._db.sync()

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, v):
        self._id = v
        self.save()

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, v):
        self._url = v
        self.save()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, v):
        self._status = v
        self.save()


class MemberDB:

    _DIALECT = 'excel-tab'

    def __init__(self, path):
        path = Path(path)
        path.touch(mode=0o600, exist_ok=False)
        self._path = path
        self._lock = mp.RLock()
        with self._lock, self._open_read() as records:
            self._records = { r['id']: r for r in records }

    def sync(self):
        with self._lock, self._open_write() as writer:
            writer.writerows(self._records.values())

    def get(self, id):
        with self._lock:
            return Member(*self._records[id], database=self)

    def create(self, id, url, status=DEAD, member_cls=None):
        with self._lock:
            if id in self._records:
                raise ValueError(f"Member(id={id}) already exists")
            record = { 'id': id, 'status': status, 'status': status }
            self._records[id] = record
            self.sync()
            return Member(*record, database=self)

    @contextmanager
    def _open_read(self):
        with open(self._path, newline='') as fs:
            yield csv.DictReader(fs, self._DIALECT)

    @contextmanager
    def _open_write(self, append=False):
        with open(self._path, 'a' if append else 'w', newline='') as fs:
            yield csv.DictWriter(fs, self._DIALECT)


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
        self._db_path = os.path.normpath(db_path)
        self._nodes_file = path_join(self._db_path, 'nodes')
        self._member_cls = import_class(member_cls)
        self._replicas = replicas

    def initdb(self):
        try:
            os.makedirs(self._db_path)
        except OSError:
            raise CommandError(
                'Cannot initialize database in existing directory'
            )
        open(self._nodes_file, 'a').close()

    def list_nodes(self):
        with _nodes_reader(self._nodes_file) as reader:
            return list(reader)

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
        with _nodes_reader(self._nodes_file) as reader:
            nodes = [x[1] for x in reader]
        for n in nodes:
            self._member_cls(n).mkfs()

    def df(self):
        with _nodes_reader(self._nodes_file) as reader:
            nodes = list(reader)
        results = []
        for id, addr in nodes:
            results.append(
                (id, self._member_cls(addr).df())
            )
        return results

    def cd(self, path: str):
        with _nodes_reader(self._nodes_file) as reader:
            nodes = list(reader)
        for id, addr in nodes:
            try:
                self._member_cls(addr).cd(path)
            except (URLError, HTTPError):
                pass

    HANDLERS = {
        '/initdb': (initdb, deserialize, serialize),
        '/nodes/list': (list_nodes, deserialize, serialize),
        '/nodes/join': (add_node, deserialize_join, serialize),
        '/nodes/leave': (exclude_node, deserialize, serialize),
    }

