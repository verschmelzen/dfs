"""
Utilities for class imports, filesystem packaging, serialization and
deserialization used in other modules.

Attributes
----------
ID_SYMBOLS : str
    Symbols used to generate data node id.
ID_LENGTH : int
    Length of data node id.
"""
import os
import sys
import csv
import random
import string
import tarfile
from typing import List, Any, Tuple
from io import BytesIO, IOBase
from importlib import import_module


def path_join(first: str, *args: List[str]) -> str:
    """
    Join paths and normalize.

    Parameters
    ----------
    first : str
        Base path.
    args : List[str]
        Path segments.

    Returns
    -------
    str:
        Normalized path.
    """
    return os.path.normpath(os.path.join(first, *args))


def import_class(path: str) -> object:
    """
    Import python class using path in format 'package.subpackage.Class'.

    Parameters
    ----------
    path : str
        Path to class in format 'package.subpackage.Class'.

    Returns
    -------
    object:
        Imported class.
    """
    parts = path.split('.')
    package_parts, klass = parts[:-1], parts[-1]
    if package_parts:
        return getattr(import_module('.'.join(package_parts)), klass)
    return getattr(sys.modules[__name__], klass)


class CommandError(Exception):
    """
    User related error to used in python part of DFS API
    """
    pass


ID_SYMBOLS = string.digits + 'abcdef'
ID_LENGTH = 6


def gen_id() -> str:
    """
    Generate data node id.

    Returns
    -------
    str:
        Id of data node.
    """
    return ''.join(random.choice(ID_SYMBOLS) for _ in range(ID_LENGTH))


def package(path: str) -> bytes:
    """
    Tarball and gzip contents under path.

    Parameters
    ----------
    path : str
        Path to directory of file to package.

    Returns
    -------
    bytes:
        Compressed tarball of given path.
    """
    packaged = BytesIO()
    with tarfile.open(fileobj=packaged, mode='w|gz') as tar:
        tar.add(path, '/')
    return packaged.getvalue()


def unpack(package: bytes, path: str):
    """
    Read package as gzip compressed tarball and extract its contents
    to path.

    Parameters
    ----------
    package : bytes
        Compressed tarball.
    path : str
        Path to extract contents.
    """
    packaged = BytesIO(package)
    with tarfile.open(fileobj=packaged, mode='r|gz') as tar:
        tar.extractall(path)


def deserialize(
    stream: IOBase,
    content_len: int,
    remote_ip: str,
) -> Any:
    """
    Deserialize stream using information from server.

    Generic deserialization for following formats:

        1. single utf-8 string without whitespaces
        2. utf-8 string and raw byte data separated by first zero byte
        3. two utf-8 string separated by first space in stream
        4. utf-8 string and flag represented by single '!' and separated
           by space

    Parameters
    ----------
    stream : IOBase
        Stream of request body.
    content_len : int
        Length of request body.
    remote_ip : str
        IP address of client.

    Returns
    -------
    Any:
        One of the options described above
    """
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


def serialize(data: Any) -> bytes:
    """
    Serialize data with generic method.

    Serialization is done by converting to string andencoding with
    utf-8 for non-iterable types, and by joining with space and utf-8
    encoding for iterable types.

    Parameters
    ----------
    data : Any
        Data to serialize.

    Returns
    -------
    bytes:
        Serialized data.
    """
    if data == None:
        return b''
    if type(data) == bytes:
        return data
    if type(data) == str:
        return data.encode('utf-8')
    try:
        iterator = iter(data)
    except TypeError:
        return str(data).encode('utf-8')
    return ' '.join(str(x) for x in iterator).encode('utf-8')


def deserialize_tuple(
    stream: IOBase,
    content_len: int,
    remote_ip: str,
) -> Tuple[int, int, int]:
    """
    Deserialize tuple returned by df.

    Parameters
    ----------
    stream : IOBase
        Stream of request body.
    content_len : int
        Length of request body.
    remote_ip : str
        IP address of client.

    Returns
    -------
    Tuple[int, int, int]:
        Total, used and free memory in bytes.
    """
    tmp = stream.read(content_len).decode('utf-8')
    total, used, free = (int(x) for x in tmp.split())
    return total, used, free


def deserialize_list(
    stream: IOBase,
    content_len: int,
    remote_ip: str,
) -> List[str]:
    """
    Deserialize list.

    Parameters
    ----------
    stream : IOBase
        Stream of request body.
    content_len : int
        Length of request body.
    remote_ip : str
        IP address of client.

    Returns
    -------
    List[str]:
        Deserialized list.
    """
    tmp = stream.read(content_len).decode('utf-8')
    return tmp.split()


def deserialize_stat(
    stream: IOBase,
    content_len: int,
    remote_ip: str,
) -> Tuple[str, int, int]:
    """
    Deserialize tuple returned by stat.

    Parameters
    ----------
    stream : IOBase
        Stream of request body.
    content_len : int
        Length of request body.
    remote_ip : str
        IP address of client.

    Returns
    -------
    Tuple[str, int, int]:
        Full path, size and mode.
    """
    tmp = stream.read(content_len).decode('utf-8')
    tmp = tmp.split()
    return tmp[0], int(tmp[1]), int(tmp[2])


def deserialize_matrix(
    stream: IOBase,
    content_len: int,
    remote_ip: str,
) -> List[List[str]]:
    """
    Deserialize list of lists of strings.

    Parameters
    ----------
    stream : IOBase
        Stream of request body.
    content_len : int
        Length of request body.
    remote_ip : str
        IP address of client.

    Returns
    -------
    List[List[str]]:
        List of lists.
    """
    tmp = stream.read(content_len).decode('utf-8')
    lines = tmp.split('\n')
    return [l.split('\t') for l in lines]


def deserialize_join(
    stream: IOBase,
    content_len: int,
    remote_ip: str,
) -> Tuple[str, str, str]:
    """
    Deserialize data for NameNode.add_node().

    Parameters
    ----------
    stream : IOBase
        Stream of request body.
    content_len : int
        Length of request body.
    remote_ip : str
        IP address of client.

    Returns
    -------
    Tuple[str, str, str]:
        Public ip, access url and id of data node.
    """
    tmp = stream.read(content_len).decode('utf-8').split(' ')
    public_url = None
    if len(tmp) > 2:
        public_url, port, id = tmp
    else:
        port, id = tmp
    if ':' in port:
        remote_ip, port = port.split(':')
    url = 'http://' + remote_ip + ':' + port + '/'
    return public_url, url, id


def serialize_matrix(data: List[List[str]]) -> bytes:
    """
    Serialize list of lists.

    Parameters
    ----------
    data : List[List[str]]
        Lists to serialize.

    Returns
    -------
    bytes:
        Serialized lists.
    """
    lines = ['\t'.join([str(y) for y in x]) for x in data]
    return '\n'.join(lines).encode('utf-8')

