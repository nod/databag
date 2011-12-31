
import json
import sqlite3
from bz2 import compress, decompress
from datetime import datetime

class DataBag(object):

    def __init__(self, fpath=':memory:', bag=None):
        if not bag: self._bag = self.__class__.__name__
        self._db = sqlite3.connect(fpath, detect_types=sqlite3.PARSE_DECLTYPES)
        self._db.row_factory = sqlite3.Row
        self._ensure_table()

    def _ensure_table(self):
        cur = self._db.cursor()
        cur.execute(
            '''create table if not exists {} (
                keyf text, data blob, ts timestamp, json boolean
                )'''.format(self._bag)
            )
        cur.execute(
            '''create unique index if not exists
                idx_dataf_{} on {} (keyf)'''.format(self._bag, self._bag)
            )

    def __getitem__(self, keyf):
        cur = self._db.cursor()
        cur.execute(
            '''select data, json from {} where keyf=?'''.format(self._bag),
            (keyf,)
            )
        d = cur.fetchone()
        if d is None: raise KeyError
        val_ = decompress(d['data'])
        return json.loads(val_) if d['json'] else val_

    def __setitem__(self, keyf, value):
        to_json = False
        if type(value) is not basestring:
            value = json.dumps(value)
            to_json = True
        cur = self._db.cursor()
        cur.execute(
            '''INSERT OR REPLACE INTO {} (keyf,data,ts, json) values (?, ?, ?, ?)
                    '''.format(self._bag),
            ( keyf, sqlite3.Binary(compress(value)), datetime.now(), to_json )
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
        cur = self._db.cursor()
        cur.execute('''select keyf from {} order by keyf'''.format(self._bag))
        for k in cur:
            yield k['keyf']

    def __contains__(self, keyf):
        cur = self._db.cursor()
        cur.execute(
            '''select keyf from {} where keyf=?'''.format(self._bag),
            (keyf,)
            )
        return cur.fetchone() is not None


