"""
Thread-safe database for name node member management.

Attributes
----------
NEW : str
    'new' - status of new nodes
ALIVE : str
    'alive' - status of alive nodes
DEAD : str
    'dead' - status of dead nodes
"""
import csv
import time
from typing import Dict, List
from threading import RLock
from pathlib import Path
from contextlib import contextmanager


def current_time() -> str:
    """
    Return current time in format '%Y-%m-%d %H:%M:%S'.

    Returns
    -------
    str:
        Current time.
    """
    return time.strftime('%Y-%m-%d %H:%M:%S')


NEW = 'new'
ALIVE = 'alive'
DEAD = 'dead'


class Member:
    """
    Data node info. Should be created through MemberDB methods.
    It allows transparent thread-safe database modification through
    python properties that aquire database lock on write.
    """

    def __init__(
        self,
        id: str,
        url: str,
        public_url: str,
        status: str,
        database,
    ):
        self._id = id
        self._url = url
        self._public_url = public_url
        self._status = status
        self._db = database

    def save(self):
        record = {
            'id': self._id,
            'url': self._url,
            'public_url': self._public_url,
            'status': self._status,
        }
        self._db.update(record)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, v):
        with self._db._lock:
            self._id = v
            self.save()

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, v):
        with self._db._lock:
            self._url = v
            self.save()

    @property
    def public_url(self):
        return self._public_url

    @public_url.setter
    def public_url(self, v):
        with self._db._lock:
            self._public_url = v
            self.save()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, v):
        with self._db._lock:
            self._status = v
            self.save()


class MemberDB:
    """
    Thread-safe database for data node information management.

    It constructs and returns Member instances, allowing reading and
    writing through them while keeping, records in sync in memory
    and on disk.

    Class Attrubutes
    ----------------
    _DIALECT : str
        Dialect used for csv formatting
    _FIELDS : List[str]
        List of fields used by csv.DictReader and csv.DictWriter
    """

    _DIALECT = 'excel-tab'
    _FIELDS = ['id', 'url', 'public_url', 'status']

    def __init__(self, path: str):
        """
        Open database.

        Parameters
        ----------
        path : str
            Path to database file.
        """
        path = Path(path)
        path.touch(mode=0o600)
        self._path = path
        self._lock = RLock()
        with self._lock, self._open_read() as records:
            self._records = { r['id']: r for r in records }

    def sync(self):
        """
        Synchronize records to disk.
        """
        with self._lock, self._open_write() as writer:
            writer.writerows(self._records.values())

    def get(self, id: str) -> Member:
        """
        Get member instance from database.

        Parameters
        ----------
        id : str
            Id of data node.
        """
        with self._lock:
            try:
                record = self._records[id]
            except KeyError:
                return None
            return Member(**record, database=self)

    def update(self, record: Dict[str, str]):
        """
        Update record with same id.

        Parameters
        ----------
        record : Dict
            Record instance to rewrite.
        """
        with self._lock:
            self._records[record['id']].update(record)
            self.sync()

    def create(
        self,
        id: str,
        url: str,
        public_url: str = None,
        status: str = NEW,
    ) -> Member:
        """
        Create new data node record.

        Parameters
        ----------
        id : str
            Id of data node.
        url : str
            URL to access data node.
        public_url : str
            URL to advertise to users of clusters.
        status : str
            Status of data node.

        Returns
        -------
        Member:
            New member instance.
        """
        with self._lock:
            record = {
                'id': id,
                'url': url,
                'public_url': public_url,
                'status': status,
            }
            self._records[id] = record
            self.sync()
            return Member(**record, database=self)

    def filter(self, **kwargs) -> List[Member]:
        """
        Find records that match the filters in kwargs.

        Returns records that exactly match value of each of the filters.

        Parameters
        ----------
        kwargs : dict
            Attributes to filter records by

        Returns
        -------
        List[Member]:
            List of member instances matching filters. Or all members
            if filters are not given.
        """
        with self._lock:
            records = [
                r for r in self._records.values()
            ]
            for (key, value) in kwargs.items():
                records = [
                    r for r in records
                    if r[key] == value
                ]
            return [Member(**r, database=self) for r in records]

    @contextmanager
    def _open_read(self):
        with open(self._path, newline='') as fs:
            yield csv.DictReader(
                fs,
                dialect=self._DIALECT,
                fieldnames=self._FIELDS,
            )

    @contextmanager
    def _open_write(self, append=False):
        with open(self._path, 'a' if append else 'w', newline='') as fs:
            yield csv.DictWriter(
                fs,
                dialect=self._DIALECT,
                fieldnames=self._FIELDS,
            )

