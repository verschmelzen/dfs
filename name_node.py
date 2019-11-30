from util import (
    CommandError,
    path_join,
    import_class,
    deserialize,
    serialize,
)


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

