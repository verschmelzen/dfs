import re
import os
import csv
from contextlib import contextmanager
from importlib import import_module

from util import CommandError, path_join, import_class


@contextmanager
def _nodes_reader(path):
    with open(path, newline='') as fs:
        try:
            yield csv.reader(fs, 'excel-tab')

@contextmanager
def _nodes_writer(path):
    with open(path, 'w', newline='') as fs:
        try:
            yield csv.writer(fs, 'excel-tab')

class NameNode:

    @staticmethod
    def get_args(env):
        return (env['DFS_DB_PATH'],)

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
        open(self._nodes_file).close()

    def list_nodes(self):
        with self._nodes_reader(self._nodes_file) as reader:
            return list(reader)

    def is_member(self, node_id):
        with self._nodes_reader(self._nodes_file) as reader:
            try:
                next(x for x in reader if x[0] == node_id)
                return True
            except StopIteration:
                return false

    def _send_mkfs(self, url):
        raise NotImplementedError

    def add_node(self, url, node_id):
        if is_member(node_id):
            raise CommandError(f'{node_id} is already a member')
        self._send_mkfs(url)
        with _nodes_writer(self._nodes_file) as writer:
            writer.writerow([node_id, url])

    def exclude_node(self, node_id):
        with open(self._nodes_file, 'r') as fs:
            lines = fs.read().splitlines()
        new_lines = [x for x in lines if not x.startswith(node_id)]
        if len(lines) == len(new_lines):
            raise CommandError(f'{node_id} is not a member')
        with open(self._nodes_file, 'w') as fs:
            fs.write('\n'.join(lines))

