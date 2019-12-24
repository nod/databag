
import json
import operator
import sqlite3
from bz2 import compress, decompress
from datetime import datetime
from uuid import uuid1 as uuid
from platform import python_version

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
    bag = DataBag('dbag', '/tmp/bag.sqlite3')
    bag['blah'] = 'blip'
    ```
    """

    def __init__(self, table=None, fpath=None, versioned=False, history=10):
        if not fpath:
            fpath=':memory:'
        self._table = table
        self._versioned = versioned
        self._history = history
        self._db = sqlite3.connect(fpath, detect_types=sqlite3.PARSE_DECLTYPES)
        self._db.row_factory = sqlite3.Row
        self._ensure_table()

    def _ensure_table(self):
        cur = self._db.cursor()
        cur.execute(
            '''create table if not exists {tbl} (
                keyf text, data blob, ts timestamp,
                json boolean, bz2 boolean, ver int
                )'''.format(tbl=self._table)
            )
        cur.execute(
            '''create unique index if not exists
                idx_dataf_{tbl} on {tbl} (keyf, ver)'''.format(tbl=self._table)
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
            from {tbl}
            where keyf=? and ver=?
            '''.format(tbl=self._table),
            (keyf, version)
            )
        d = cur.fetchone()
        if d is None: raise KeyError
        return self._data(d)

    def _data(self, d):
        val_ = decompress(d['data']).decode() if d['bz2'] else d['data']
        return json.loads(val_) if d['json'] else val_

    def _genkey(self):
        return hashint(uuid().int)

    def add(self, value):
        k = self._genkey()
        self[k] = value
        return k

    def __setitem__(self, keyf, value):
        to_json = is_bz2 = False
        if not isinstance(value, str):
            dtjs = lambda d: d.isoformat() if isinstance(d, datetime) else None
            value = json.dumps(value, default=dtjs)
            to_json = True

        if len(value) > 39: # min len of bz2'd string
            compressed = compress(value.encode())
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
                from {tbl} where keyf=?
                order by ver asc
                '''.format(tbl=self._table),
                (keyf,)
                )
            for r in curv:
                ver = r['ver'] - 1
                if abs(ver) > self._history:
                    # poor fella, getting whacked
                    cur.execute('''
                        delete from {tbl} where keyf=? and ver=?
                        '''.format(tbl=self._table),
                        (keyf, r['ver'])
                        )
                else:
                    cur.execute('''
                        update {tbl} set ver=? where keyf=? and ver=?
                        '''.format(tbl=self._table),
                        (ver, keyf, r['ver'])
                        )
        else:
            cur.execute('''
                delete from {tbl} where keyf=? and ver=0
                '''.format(tbl=self._table),
                (keyf,)
                )

        cur.execute(
            '''INSERT INTO {tbl} (keyf, data, ts, json, bz2, ver)
                values (?, ?, ?, ?, ?, 0)'''.format(tbl=self._table),
            ( keyf, value, datetime.now(), to_json, is_bz2 )
            )
        self._db.commit()

    def __delitem__(self, keyf):
        """
        remove an item from the bag, all versions if exist.
        """
        cur = self._db.cursor()
        cur.execute(
            '''delete from {tbl} where keyf = ?'''.format(tbl=self._table),
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
            '''select ts from {tbl} where keyf=?'''.format(tbl=self._table),
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
        cur.execute('''select keyf from {tbl} order by keyf'''.format(
            tbl=self._table
        ) )
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
                from {tbl} order by ts {o}'''.format(
                    tbl=self._table, o=order
                    )
            )
        for d in cur:
            yield d['keyf'], self._data(d)

    def __contains__(self, keyf):
        cur = self._db.cursor()
        cur.execute(
            '''select 1 from {tbl} where keyf=?'''.format(tbl=self._table),
            (keyf,)
            )
        return cur.fetchone() is not None


class Qmeta(type):
    """ allows us some syntactic sugar for attr access """
    def __getattr__(cls, key):
        return Q(key)


class Q(metaclass=Qmeta):

    """
    builds queries for dictbag instances using find()

    NOTE - if there aren't indexes on the searched fields, this is SLOW


    ```python
    d = DictBag('dbag')
    d.add({'x':23}))
    for i in d.find(Q('x')>20, Q('x')<30):
        ...
    ```

    NOTE: multiple Q's on the same key get and'd.  OR isn't supported yet.

    You can also create one q object for your query on the same key and use that
    over and over.
    ```python
    d = DictBag('dbag')
    d.add({'x':23}))
    x = Q('x')
    for i in d.find(20<x<30):
        ...

    ```
    """

    ops = {
        '>': operator.gt,
        '<': operator.lt,
        '>=': operator.ge,
        '<=': operator.le,
        '=': operator.eq,
        '!=': operator.ne,
        }

    def __init__(self, key):
        self._k = key
        self._ands = list()
        self._and_ops = set()

    def query(self):
        return (
            "and".join(
                ' "{k}" {v} ? '.format(k=self._k, v=op) for op,_ in self._ands
            ),
            [v for _,v in self._ands]
        )

    @property
    def key(self):
        return self._k

    # we have to use a list instead of a set on the _ands because in order to
    # test the generated sql, we need to be able to predict the order of the
    # params output and a set() lacks that attribute

    def _cond(self, sym, val):
        """ builds the anded sets of sql syntax and operations for the query """
        cond = (sym, val)
        op = Q.ops.get(sym)
        if cond not in self._ands:
            self._ands.append(cond)
            self._and_ops.add( (op, val) )
        return self

    def __lt__(self, val):
        return self._cond( '<', val )

    def __le__(self, val):
        return self._cond( '<=', val )

    def __gt__(self, val):
        return self._cond( '>', val )

    def __ge__(self, val):
        return self._cond( '>=', val )

    def __eq__(self, val):
        return self._cond( '=', val )

    def __ne__(self, val):
        return self._cond( '!=', val)


class DictBag(DataBag):
    """
    convenience bag for dictionaries that adds an index field.  This allows
    ranged retrievals based on queries on indexed fields.


    NOTE - the entire index model here is heavily inspired by goatfish
    """

    def __init__(self, table=None, fpath=None, indexes=None):

        super(DictBag, self).__init__(table=table, fpath=fpath)
        self._indexes = set()
        if indexes:
            for idx in indexes:
                self.ensure_index(idx)

    def _make_index_name(self, index):
        nm = '_'.join(sorted(index))
        return 'idx_{t}_{x}'.format(t=self._table, x=nm)

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

        cols = [' "{i}" '.format(i=idx) for idx in index]


        # big gross note - notice the column type on the index table value
        # field, it's real.  if we try to genericize it to TEXT then sqlite does
        # numeric comparisons based on text values (meaning things like 500 < 9)
        # since sqlite3 is a pretty forgiving db, we can splat all sorts of data
        # into a REAL column and it gets stored and treated properly.  by saying
        # it's REAL we basically say "try to be constrained as possible".
        # Is this a hack? yep.  Does it work? yep.

        cur = self._db.cursor()
        cur.execute(
            '''create table if not exists {i} (
                "id" integer primary key autoincrement not null,
                "keyf" text,
                {j}
                )'''.format(
                    i=idx_name,
                    j=",".join( " {} real ".format(c) for c in cols )
                    )
            )
        cur.execute(
            '''create index if not exists
                i_{i} on {i} ({c})'''.format(i=idx_name, c=','.join(cols))
            )
        self._db.commit()
        self._indexes.add(tuple(sorted( index )))

    def __setitem__(self, keyf, value):
        if not isinstance(value, dict):
            raise ValueError('dictbags are for dicts')

        # save it normally as expected
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
            insert into {i} (keyf, {k}) values ({v})
            '''.format(
                    i=idx,
                    k=', '.join('"{}"'.format(i) for i in index),
                    v=', '.join(['?']*len(vals))
                ),
            vals
            )
        self._db.commit()

    def find_one(self, *a, **ka):
        """
        returns a (key,dict) for the matching query, or (None,None) if not
        found in the bag
        """
        try:
            return next(self.find(*a, **ka))
        except StopIteration:
            return None, None

    def _find_matching_index(self, cols):
        # find largest index match
        # treat cols as a set and find the largest intersection
        # NOTE - for now, if an index doesn't contain all the keys to query
        #        against, an index is not included

        if self._indexes:
            colset = set(cols)
            matching_index, score = max(
                ((i,len(colset.intersection(i))) for i in self._indexes),
                key=lambda ii:ii[1]
                )
            if score >= len(cols):
                return matching_index
        return None

    def _slow_search(self, qs):
        """
        perform an iteration over the entire set and return anything matching
        the query filter.

        NOTE - we got here because there wasn't an existing index covering ALL
            key filters
        OPTIMIZE - we should use any partial indexes to reduce our potential
            corpus, then iterate over that
        """
        for k,d in self.by_created(desc=True):
            # rip through each document in the db...
            # performing the queries on each one
            matched_all = True
            for q in qs:
                # on each document, first see if the key even exists
                qk = q.key
                if qk not in d:
                    matched_all = False
                    break

                # now check each query against the doc
                dv = d.get(qk)
                for op, val in q._and_ops:
                    if not op(dv, val):
                        matched_all = False
                        break
                # if not all( op(dv, val) for op,val in q._and_ops ):
                    # matched_all = False
                    # break
            if matched_all:
                # if we got here, we matched all queries
                yield k, d

    def _findQ(self, *a, **ka):
        """
        accepts keyword arguments for eq matches on symbols.  Also accepts
        Q arguments for filtering.

        acts as generator of results
        """

        # shortcircuit for empty a and ka
        if not a and not ka:
            for k,data in self.by_created(desc=True):
                yield k,data
            return

        qs = []

        # first, let's do the keyword args, those are straightforward
        for k,v in ka.items():
            qs.append( Q(k) == v )

        # now let's build the query objects, *a should be a list of Q objs
        qs.extend(a)

        colset = set( q.key for q in qs )
        index = self._find_matching_index(colset)

        if not index:
            # OPTIMIZE
            # gotta do it the slow way...
            # note, this could be massively optimized but at the moment if
            # there's not an index for every queried column, we will do it the
            # slow way for everything. We should, if we find a partial index, do
            # a limited search then drop to iteration for the remaining query
            # filter
            for k,doc in self._slow_search(qs):
                yield k,doc
            return

        idxname = self._make_index_name(index)

        where, params = [], []
        for q in qs:
            w,p = q.query()
            where.append(w)
            params.extend(p)

        cur = self._db.cursor()
        rows = cur.execute(
            '''
            select db.keyf as k, db.data, db.bz2, db.json
            from "{t}" as db
            where exists (
                select 1 from "{i}" as idx
                where idx.keyf = db.keyf and {w}
                )
            '''.format(t=self._table, i=idxname, w=' and '.join( where ) ),
            params
            )
        # return ( (d['k'], self._data(d)) for d in rows )
        for d in rows:
            yield d['k'], self._data(d)

    def _search_query(self, qdict):
        """ returns Q object """
        # {'y':111, 'x':{'$lt':23}}

        # build the keyword->operator mapping.
        # we could do trixy stuff here but i like the explicit listing
        # of what's supported so nothing gets slipped in, module wise
        ops = {
            '$gt': operator.gt,
            '$lt': operator.lt,
            '$gte': operator.ge,
            '$lte': operator.le,
            '$ne': operator.ne,
            }

        qs = []

        for k,v in qdict.items():
            if not isinstance(v, dict):
                # treat like normal keyword match {'y':111}
                qs.append( Q(k) == v)
            else:
                # OPTIMIZE - does not check for formatting other than an
                # existing op...
                # dictionary.. for now assume it's formatted properly
                for o_, val in v.items():
                    if o_ not in ops:
                        raise NotImplementedError(
                            "Non-supported operator detected: " + k
                        )
                    op = ops[o_]
                    qs.append( op(Q(k), val) )
        return qs

    def find(self, *qdicts, **kwa):
        """
        finds things in the bag, acts as a generator of results on a filter

        You can find things via keyword:
        ```
        >>> x = DictBag('dbag')
        >>> id_ = x.add({'k1':23, 'k2':88})
        >>> x.find(k2=88).next()
        {u'k2': 88, u'k1': 23}

        And via Q objects
        >>> x.find( Q('k1') > 20 )
        {u'k2': 88, u'k1': 23}

        And also with dict queries
        >>> x.find( {'k1': {'$gt':20} } )
        {u'k2': 88, u'k1': 23}

        And even a combination of ...
        >>> x.find( k2=88, {'k1': 23})
        ```
        """
        qs = []
        for qd in qdicts:
            if isinstance(qd, Q):
                # Q objects just get appended directly
                qs.append( qd )
            elif isinstance(qd, dict):
                # if it's a query dict, convert to Q object first
                qs.extend( self._search_query(qd) )
            else:
                # I DON'T KNOW YOU
                raise TypeError('query must be dict or Q object')
        return self._findQ( *qs, **kwa )

