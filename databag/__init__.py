
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


class DataBag(object):
    """
    put your data in a bag.

    ```python
    bag = DataBag('/tmp/bag.sqlite3')
    bag['blah'] = 'blip'
    ```
    """

    def __init__(self, fpath=None, bag=None, versioned=False, history=10):

        if not fpath:
            fpath=':memory:'

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


class Q(object):

    """
    builds queries for dictbag instances using find()

    NOTE - if there aren't indexes on the searched fields, this is SLOW


    ```python
    d = DictBag( )
    d.add({'x':23}))
    for i in d.find(Q('x')>20, Q('x')<30):
        ...
    ```

    NOTE: multiple Q's on the same key get and'd.  OR isn't supported yet.

    You can also create one q object for your query on the same key and use that
    over and over.
    ```python
    d = DictBag( )
    d.add({'x':23}))
    x = Q('x')
    for i in d.find(20<x<30):
        ...

    ```
    """


    def __init__(self, key):
        self._k = key
        self._ands = []

    def query(self):
        return (
            "and".join(
                ' "{}" {} ? '.format(self._k, op) for op,_ in self._ands
            ),
            [v for _,v in self._ands]
        )

    @property
    def key(self):
        return self._k

    def __lt__(self, val):
        self._ands.append(( '<', val ))
        return self

    def __le__(self, val):
        self._ands.append(( '<=', val ))
        return self

    def __gt__(self, val):
        self._ands.append(( '>', val ))
        return self

    def __ge__(self, val):
        self._ands.append(( '>=', val ))
        return self

    def __eq__(self, val):
        self._ands.append(( '=', val ))
        return self

class IndexNotFound(Exception):
    """
    raised when a query occurs on cols w/o an index
    """

class DictBag(DataBag):
    """
    convenience bag for dictionaries that adds an index field.  This allows
    ranged retrievals based on queries on indexed fields.


    NOTE - the entire index model here is heavily inspired by goatfish
    """

    def __init__(self, fpath=None, bag=None, indexes=None):

        super(DictBag, self).__init__(fpath=fpath, bag=bag)
        self._indexes = set()
        if indexes:
            for idx in indexes:
                self.ensure_index(idx)

    def _make_index_name(self, index):
        nm = '_'.join(sorted(index))
        return 'idx_{}_{}'.format(self._bag, nm)

    def ensure_index(self, index):
        """
        creates an index on a set of fields in a dict

        Notes
        - these can be considered sparse.  If a key doesn't exist in a dictionary,
          it won't be added to the index.
        - currently, if an item is added before an index is created, the item
          isn't in the index. this is gross and will be fixed soon.
        """
        idx_name = self._make_index_name(index)

        cols = [' "{}" '.format(idx) for idx in index]

        cur = self._db.cursor()
        cur.execute(
            '''create table if not exists {} (
                "id" integer primary key autoincrement not null,
                "keyf" text,
                {}
                )'''.format(
                    idx_name,
                    ",".join( " {} text ".format(c) for c in cols )
                    )
            )
        cur.execute(
            '''create index if not exists
                i_{} on {} ({})'''.format(idx_name, idx_name, ','.join(cols))
            )
        self._db.commit()
        self._indexes.add(tuple(sorted( index )))

    def __setitem__(self, keyf, value):

        if not isinstance(value, dict):
            raise ValueError('dictbags are for dicts')

        # save it normally expected
        super(DictBag, self).__setitem__(keyf, value)

        # now add it to the necessary indexes
        for i in self._indexes:
            self._add_to_index(keyf, value, i)

    def _add_to_index(self, key, data, index):
        keys = set(data.keys())
        if not keys.intersection(index): return

        idx = self._make_index_name(index)
        cur = self._db.cursor()
        vals = [ data.get(i, None) for i in index ]
        vals.insert(0, key)
        cur.execute(
            '''
            insert into {} (keyf, {}) values ({})
            '''.format(
                    idx,
                    ', '.join('"{}"'.format(i) for i in index),
                    ', '.join(['?']*len(vals))
                ),
            vals
            )
        self._db.commit()

    def findone(self, *a, **ka):
        return self.find(*a, **ka).next()

    def _find_matching_index(self, cols):
        # find largest index match
        # treat cols as a set and find the largest intersection
        # NOTE - for now, if an index doesn't contain all the keys to query
        #        against, an index is not included
        colset = set(cols)
        matching_index, score = max(
            ((i,len(colset.intersection(i))) for i in self._indexes),
            key=lambda ii:ii[1]
            )
        if score >= len(cols):
            return matching_index
        return None

    def find(self, *a, **ka):
        """
        finds things in the bag

        You can find things via keyword:
        ```
        >>> x = DictBag()
        >>> x.ensure_index(('k1', 'k2'))
        >>> x.add({'k1':23, 'k2':88})
        '6ZjcPHsY4WPn63ygxTwpCR'
        >>> x.find(k2=88).next()
        {u'k2': 88, u'k1': 23}
        ```
        """

        qs = []

        # first, let's do the keyword args, those are straightforward
        for k,v in ka.iteritems():
            qs.append( Q(k) == v )


        # now let's build the query objects
        qs.extend(a)


        colset = set( q.key for q in qs )
        index = self._find_matching_index(colset)

        if not index: raise IndexNotFound()
        idxname = self._make_index_name(index)

        cur = self._db.cursor()

        where, params = [], []
        for q in qs:
            w,p = q.query()
            where.append(w)
            params.extend(p)

        rows = cur.execute(
            '''
            select db.keyf as k, db.data, db.bz2, db.json
            from "{}" as db
            where exists (
                select 1 from "{}" as idx
                where idx.keyf = db.keyf and {}
                )
            '''.format(self._bag, idxname, ' and '.join( where ) ),
            params
            )
        return ( (d['k'], self._data(d)) for d in rows )

