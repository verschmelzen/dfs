import csv
import time
from threading import RLock
from pathlib import Path
from contextlib import contextmanager


def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S')


NEW = 'new'
HEALTHY = 'healty'
DEAD = 'dead'


class Member:

    def __init__(self, id, url, status, last_alive, database):
        self._id = id
        self._url = url
        self._status = status
        self._last_alive = last_alive
        self._db = database

    def save(self):
        self._db.sync()

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
    def status(self):
        return self._status

    @status.setter
    def status(self, v):
        with self._db._lock:
            self._status = v
            self.save()

    @property
    def last_alive(self):
        return self._last_alive

    @last_alive.setter
    def last_alive(self, v):
        with self._db._lock:
            self._last_alive = v
            self.save()


class MemberDB:

    _DIALECT = 'excel-tab'
    _FIELDS = ['id', 'url', 'status', 'last_alive']

    def __init__(self, path):
        path = Path(path)
        path.touch(mode=0o600, exist_ok=False)
        self._path = path
        self._lock = RLock()
        with self._lock, self._open_read() as records:
            self._records = { r['id']: r for r in records }

    def sync(self):
        with self._lock, self._open_write() as writer:
            writer.writerows(self._records.values())

    def get(self, id):
        with self._lock:
            return Member(*self._records[id], database=self)

    def create(self, id, url, status=NEW, last_alive=None, member_cls=None):
        with self._lock:
            if id in self._records:
                raise ValueError(f"Member(id='{id}') already exists")
            if not last_alive:
                last_alive = current_time()
            record = {
                'id': id,
                'url': url,
                'status': status,
                'last_alive': last_alive,
            }
            self._records[id] = record
            self.sync()
            return Member(*record, database=self)

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

