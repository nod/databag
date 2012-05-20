
import json
import sqlite3
from bz2 import compress, decompress
from datetime import datetime
from uuid import uuid1 as uuid


# hash any int to about a b64ish
CHARSET = '0123456789abcdefghjklmnopqrstvwxyzABCDEFGHJKLMNOPQRSTVWXYZ'
BASE = len(CHARSET)
def hashint(num):
    if num == 0: return '0'
    if num < 0:
        sign = '-'
        num = -num
    else:
        sign = ''
    result = ''
    while num:
        result = CHARSET[num % (BASE)] + result
        num //= BASE
    return sign + result


# convenience class shamelessly borrowed from the tornado project
# https://github.com/facebook/tornado/blob/master/tornado/util.py
class ObjectDict(dict):
    """
    Makes a dictionary behave like an object.

    >>> o = ObjectDict({'x':'xyz'})
    >>> o.x
    'xyz'
    """
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value


class DataBag(object):
    """
    put your data in a bag.

    ```python
    bag = DataBag('/tmp/bag.sqlite3')
    bag['blah'] = 'blip'
    """

    def __init__(self, fpath=':memory:', bag=None, versioned=False, history=10):

        # set the table name we'll be storing in
        if isinstance(bag, basestring): self._bag = bag
        else: self._bag = self.__class__.__name__

        self._versioned = versioned
        self._history = history

        self._db = sqlite3.connect(fpath, detect_types=sqlite3.PARSE_DECLTYPES)
        self._db.row_factory = sqlite3.Row
        self._ensure_table()

    def _ensure_table(self):
        cur = self._db.cursor()
        cur.execute(
            '''create table if not exists {} (
                keyf text, data blob, ts timestamp,
                json boolean, bz2 boolean, ver int
                )'''.format(self._bag)
            )
        cur.execute(
            '''create unique index if not exists
                idx_dataf_{} on {} (keyf, ver)'''.format(self._bag, self._bag)
            )
        self._db.commit()

    def _check_version_arg(self, v):
        if v is None: return 0
        if not isinstance(v, int) and not v < 1:
            raise IndexError('version must be 0 or less')
        return v

    def get(self, keyf, default=None, version=None):
        """
        implements an alternative method for retrieving items from the bag
        """
        version = self._check_version_arg(version)
        if keyf not in self:
            return default
        return self.__getitem__(keyf, version)

    def __getitem__(self, keyf, version=None):
        version = self._check_version_arg(version)
        cur = self._db.cursor()
        cur.execute(
            '''
            select data, json, bz2
            from {}
            where keyf=? and ver=?
            '''.format(self._bag),
            (keyf, version)
            )
        d = cur.fetchone()
        if d is None: raise KeyError
        return self._data(d)

    def _data(self, d):
        val_ = decompress(d['data']) if d['bz2'] else d['data']
        return json.loads(val_) if d['json'] else val_

    def _genkey(self):
        return hashint(uuid().int)

    def add(self, value):
        k = self._genkey()
        self[k] = value
        return k

    def __setitem__(self, keyf, value):
        to_json = is_bz2 = False
        if type(value) is not basestring:
            dtjs = lambda d: d.isoformat() if isinstance(d, datetime) else None
            value = json.dumps(value, default=dtjs)
            to_json = True

        if len(value) > 39: # min len of bz2'd string
            compressed = compress(value)
            if len(value) > len(compressed):
                value = sqlite3.Binary(compressed)
                is_bz2 = True

        # we'll want this handle to not scope out in a minute so that it gets
        # commited if we are versioned
        cur = self._db.cursor()

        # handle versioning
        if self._versioned:
            curv = self._db.cursor()
            curv.execute('''
                select ver
                from {} where keyf=?
                order by ver asc
                '''.format(self._bag),
                (keyf,)
                )
            for r in curv:
                ver = r['ver'] - 1
                if abs(ver) > self._history:
                    # poor fella, getting whacked
                    cur.execute('''
                        delete from {} where keyf=? and ver=?
                        '''.format(self._bag),
                        (keyf, r['ver'])
                        )
                else:
                    cur.execute('''
                        update {} set ver=? where keyf=? and ver=?
                        '''.format(self._bag),
                        (ver, keyf, r['ver'])
                        )
        else:
            cur.execute('''
                delete from {} where keyf=? and ver=0
                '''.format(self._bag),
                (keyf,)
                )

        cur.execute(
            '''INSERT INTO {} (keyf, data, ts, json, bz2, ver)
                values (?, ?, ?, ?, ?, 0)'''.format(self._bag),
            ( keyf, value, datetime.now(), to_json, is_bz2 )
            )
        self._db.commit()

    def __delitem__(self, keyf):
        """
        remove an item from the bag, all versions if exist.
        """
        cur = self._db.cursor()
        cur.execute(
            '''delete from {} where keyf = ?'''.format(self._bag),
            (keyf,)
            )
        # raise error if nothing deleted
        if cur.rowcount != 1:
            raise KeyError
        self._db.commit()

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
        returns key,value from bag in date order
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
            '''select 1 from {} where keyf=?'''.format(self._bag),
            (keyf,)
            )
        return cur.fetchone() is not None


