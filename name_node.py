import re
import os
import csv
from contextlib import contextmanager
from importlib import import_module

from util import CommandError, path_join, import_class, deserialize, serialize


@contextmanager
def _nodes_reader(path):
    with open(path, newline='') as fs:
        yield csv.reader(fs, 'excel-tab')


@contextmanager
def _nodes_writer(path):
    with open(path, 'w', newline='') as fs:
        yield csv.writer(fs, 'excel-tab')


def deserialize_join(stream, content_len, remote_ip):
    return stream.read(content_len).decode('utf-8'), remote_ip


class NameNode:

    @staticmethod
    def get_args(env):
        return (env['DFS_DB_PATH'], env['DFS_MEMBER_CLS'])

    def __init__(self, db_path, member_cls):
        self._db_path = os.path.normpath(db_path)
        self._nodes_file = path_join(self._db_path, 'nodes')
        self._member_cls = import_class(member_cls)

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
        with _nodes_writer(self._nodes_file) as writer:
            writer.writerow([node_id, url])

    def exclude_node(self, node_id):
        with _nodes_reader(self._nodes_file) as reader:
            records = [x for x in reader]
        new_records = [x for x in records if not x[0] == node_id]
        if len(records) == len(new_records):
            raise CommandError(f'{node_id} is not a member')
        with _nodes_writer(self._nodes_file) as fs:
            fs.write('\n'.join(records))

    HANDLERS = {
        '/initdb': (initdb, deserialize, serialize),
        '/nodes/join': (add_node, deserialize_join, serialize),
        '/nodes/leave': (exclude_node, deserialize, serialize),
    }

