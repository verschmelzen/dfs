import os
import sys
from importlib import import_module
from wsgiref.simple_server import make_server
from wsgiref.util import application_uri


def import_class(path):
    parts = path.split('.')
    package_parts, klass = parts[:-1], parts[-1]
    if package_parts:
        return getattr(import_module('.'.join(package_parts)), klass)
    return getattr(sys.modules[__name__], klass)


def route_request(env, start_request):
    node = env['DFS_NODE_CLASS']
    url = application_uri(env).rstrip('/')
    method, deserialize = node.URLS[url]
    method(*deserialize(env['wsgi.input']))
    # TODO: write response


if __name__ == '__main__':
    node = import_class(
        os.environ['DFS_NODE_CLASS']
    )(os.environ['DFS_FS_ROOT'])

    def wsgi_app(env, start_request):
        env['DFS_NODE_CLASS'] = node
        return route_request(env, start_request)

    with make_server(
        os.environ['DFS_HOST'],
        int(os.environ['DFS_PORT']),
        wsgi_app,
    ) as server:
        server.serve_forever()

