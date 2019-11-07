import os
import sys
import csv
import random
import string
from importlib import import_module


def path_join(first, *args):
    return os.path.normpath(
        os.path.join(first, *(a.strip('/') for a in args))
    )


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

