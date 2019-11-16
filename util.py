import os
import sys
import csv
import random
import string
from importlib import import_module


def path_join(first, *args):
    return os.path.normpath(os.path.join(first, *args))


def import_class(path):
    parts = path.split('.')
    package_parts, klass = parts[:-1], parts[-1]
    if package_parts:
        return getattr(import_module('.'.join(package_parts)), klass)
    return getattr(sys.modules[__name__], klass)


class CommandError(Exception):
    pass


ID_SYMBOLS = string.ascii_lowercase + string.digits
ID_LENGTH = 6


def gen_id():
    return ''.join(random.choice(ID_SYMBOLS) for _ in range(6))


def deserialize(stream, content_len, remote_ip):
    it = iter(stream.read(content_len))
    has_blob = False
    path = b''
    for b in it:
        b = bytes([b])
        if b == b' ':
            break
        if b == b'\0':
            has_blob = True
            break
        path += b
    else:
        return (path.decode('utf-8'),) if path else ()
    path = path.decode('utf-8')

    if has_blob:
        return path, bytes(it)

    b = bytes([next(it)])
    if b == b'!':
        nb = bytes([nnext(it)])
        if not nb:
            return path, True
        b += nb
    b += bytes(it)
    return path, b.decode('utf-8')

def serialize(data):
    if data == None:
        return b''
    if type(data) == bytes:
        return data
    try:
        iterator = iter(data)
    except TypeError:
        return str(data).encode('utf-8')
    return ' '.join(str(x) for x in iterator).encode('utf-8')

