import os
import sys
from importlib import import_module
from wsgiref.simple_server import make_server
from wsgiref.util import request_uri
from urllib.parse import urlparse

from util import CommandError


def import_class(path):
    parts = path.split('.')
    package_parts, klass = parts[:-1], parts[-1]
    if package_parts:
        return getattr(import_module('.'.join(package_parts)), klass)
    return getattr(sys.modules[__name__], klass)


def route_request(env, start_response):
    node = env['DFS_NODE_CLASS']
    uri = request_uri(env).rstrip('/')
    path = urlparse(uri).path
    command, deserialize, serialize = node.PATHS[path]
    args = deserialize(env['wsgi.input'])
    try:
        resp = command(node, *args)
    except CommandError as e:
        start_response(
            '400 Bad Request',
            [('Content-type', 'text/plain')],
        )
        return [str(e).enode('utf-8')]
    return [serialize(resp)]


if __name__ == '__main__':
    node = import_class(
        os.environ['DFS_NODE_CLASS']
    )(os.environ['DFS_FS_ROOT'])

    def wsgi_app(env, start_response):
        env['DFS_NODE_CLASS'] = node
        return route_request(env, start_response)

    with make_server(
        os.environ.get('DFS_HOST', '0.0.0.0'),
        int(os.environ.get('DFS_PORT', '8180')),
        wsgi_app,
    ) as server:
        server.serve_forever()

