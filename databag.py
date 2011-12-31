
import json
import sqlite3
from bz2 import compress, decompress
from datetime import datetime

class DataBag(object):
    """
    put your data in a bag.

    ```python
    bag = DataBag('/tmp/bag.sqlite3')
    bag['blah'] = 'blip'
    """

    def __init__(self, fpath=':memory:', bag=None):
        if not bag: self._bag = self.__class__.__name__
        self._db = sqlite3.connect(fpath, detect_types=sqlite3.PARSE_DECLTYPES)
        self._db.row_factory = sqlite3.Row
        self._ensure_table()

    def _ensure_table(self):
        cur = self._db.cursor()
        cur.execute(
            '''create table if not exists {} (
                keyf text, data blob, ts timestamp, json boolean, bz2 boolean
                )'''.format(self._bag)
            )
        cur.execute(
            '''create unique index if not exists
                idx_dataf_{} on {} (keyf)'''.format(self._bag, self._bag)
            )

    def __getitem__(self, keyf):
        cur = self._db.cursor()
        cur.execute(
            '''select data, json, bz2 from {} where keyf=?'''.format(self._bag),
            (keyf,)
            )
        d = cur.fetchone()
        if d is None: raise KeyError
        return self._data(d)

    def _data(self, d):
        val_ = decompress(d['data']) if d['bz2'] else d['data']
        return json.loads(val_) if d['json'] else val_

    def __setitem__(self, keyf, value):
        to_json = is_bz2 = False
        if type(value) is not basestring:

            dtjs = lambda d: d.isoformat() if isinstance(d, datetime) else None
            value = json.dumps(value, default=dtjs)
            to_json = True
        cur = self._db.cursor()
        if len(value) > 39: # min len of bz2'd string
            compressed = compress(value)
            if len(value) > len(compressed):
                value = sqlite3.Binary(compressed)
                is_bz2 = True
        cur.execute(
            '''INSERT OR REPLACE INTO {} (keyf, data, ts, json, bz2)
                values (?, ?, ?, ?, ?)'''.format(self._bag),
            ( keyf, value, datetime.now(), to_json, is_bz2 )
            )

    def __delitem__(self, keyf):
        """
        remove an item from the bag
        """
        cur = self._db.cursor()
        cur.execute(
            '''delete from {} where keyf = ?'''.format(self._bag),
            (keyf,)
            )
        # raise error if nothing deleted
        if cur.rowcount != 1:
            raise KeyError

    def when(self, keyf):
        """
        returns a datetime obj representing the creation of the keyed data
        """
        cur = self._db.cursor()
        cur.execute(
            '''select ts from {} where keyf=?'''.format(self._bag),
            (keyf,)
            )
        d = cur.fetchone()
        if d is None: raise KeyError
        return d['ts']

    def __iter__(self):
        """
        returns keys of items in bag, sorted by key
        """
        cur = self._db.cursor()
        cur.execute('''select keyf from {} order by keyf'''.format(self._bag))
        for k in cur:
            yield k['keyf']

    def by_created(self, desc=False):
        """
        returns key,valu from bag in date order
        """
        cur = self._db.cursor()
        order = 'desc' if desc else 'asc'
        cur.execute(
            '''select keyf, data, json, bz2
                from {} order by ts {}'''.format(self._bag, order)
            )
        for d in cur:
            yield d['keyf'], self._data(d)


    def __contains__(self, keyf):
        cur = self._db.cursor()
        cur.execute(
            '''select keyf from {} where keyf=?'''.format(self._bag),
            (keyf,)
            )
        return cur.fetchone() is not None


