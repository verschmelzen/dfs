import csv
import time
from threading import RLock
from pathlib import Path
from contextlib import contextmanager


def current_time():
    return time.strftime('%Y-%m-%d %H:%M:%S')


NEW = 'new'
ALIVE = 'alive'
DEAD = 'dead'


class Member:

    def __init__(self, id, url, public_url, status, database):
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

    _DIALECT = 'excel-tab'
    _FIELDS = ['id', 'url', 'public_url', 'status']

    def __init__(self, path):
        path = Path(path)
        path.touch(mode=0o600)
        self._path = path
        self._lock = RLock()
        with self._lock, self._open_read() as records:
            self._records = { r['id']: r for r in records }

    def sync(self):
        with self._lock, self._open_write() as writer:
            writer.writerows(self._records.values())

    def get(self, id):
        with self._lock:
            try:
                record = self._records[id]
            except KeyError:
                return None
            return Member(**record, database=self)

    def update(self, record):
        with self._lock:
            self._records[record['id']].update(record)
            self.sync()

    def create(self, id, url, public_url=None, status=NEW):
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

    def filter(self, **kwargs):
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

