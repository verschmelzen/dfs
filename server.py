import os
from wsgiref.simple_server import make_server
from wsgiref.util import request_uri
from urllib.parse import urlparse

from util import CommandError, import_class


def route_request(env, start_response):
    node = env['DFS_NODE_CLASS']
    uri = request_uri(env).rstrip('/')
    path = urlparse(uri).path
    command, deserialize, serialize = node.HANDLERS[path]
    args = deserialize(
        env['wsgi.input'],
        int(env.get('CONTENT_LENGTH', 0) or 0),
    )
    try:
        resp = command(node, *args)
    except CommandError as e:
        start_response(
            '400 Bad Request',
            [('Content-type', 'text/plain')],
        )
        return [str(e).encode('utf-8')]
    start_response(
        '200 OK',
        [('Content-type', 'application/octet-stream')],
    )
    return [serialize(resp)]


if __name__ == '__main__':
    node_cls = import_class(os.environ['DFS_NODE_CLASS'])
    node = node_cls(*node_cls.get_args(os.environ))

    def wsgi_app(env, start_response):
        env['DFS_NODE_CLASS'] = node
        return route_request(env, start_response)

    with make_server(
        os.environ.get('DFS_HOST', '0.0.0.0'),
        int(os.environ.get('DFS_PORT', '8180')),
        wsgi_app,
    ) as server:
        server.serve_forever()

