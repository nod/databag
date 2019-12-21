
import operator
import sqlite3
import unittest
from datetime import datetime
from random import shuffle
from string import ascii_letters as letters

from databag import DataBag, DictBag, Q


class TestDataBag(unittest.TestCase):

    def setUp(self):
        self.dbag = DataBag('dbag')

    def test_ensure_table(self):
        cur = self.dbag._db.cursor()

        cur.execute(
            '''SELECT name FROM sqlite_master WHERE type='table' AND name=?''',
            (self.dbag._table,)
            )
        self.assertEqual(cur.fetchone()['name'], self.dbag._table)

        # check the unique index
        sql = '''insert into {} (keyf, data, ver) values ('xx','zzz', 0)
              '''.format( self.dbag._table )
        cur.execute(sql)
        with self.assertRaises(sqlite3.IntegrityError): cur.execute(sql)

    def test_get(self):
        k,val = 'blah', 'blip'
        self.dbag[k] = val
        self.assertEqual( None, self.dbag.get('whack') )
        self.assertEqual( 'soup', self.dbag.get('whack', 'soup') )
        self.assertEqual( 'soup', self.dbag.get('whack', default='soup') )
        self.assertEqual( val, self.dbag.get(k) )

    def test_versioning(self):
        d_v = DataBag(versioned=True, history=2)
        key, orig_text, new_text = 'versioned key', 'blah', 'blip'
        d_v[key] = orig_text
        d_v[key] = new_text

        old = d_v.get(key, version=-1)
        self.assertEqual( old, orig_text )
        self.assertEqual( d_v[key], new_text )

        # now just create more versions past the history size of 2
        # exceptions will rise if this fails
        for x in range(1,10):
            d_v[key] = 'again'*x

    def test_add_no_key(self):
        val = 'jabberwocky'
        k = self.dbag.add(val)
        self.assertEqual(val, self.dbag[k])

    def test_set_get(self):
        k,val = 'zz', 'more stuff'
        self.dbag[k] = val
        self.assertEqual(val, self.dbag[k])

    def test_set_int(self):
        k,val = 'abc', 555
        self.dbag[k] = val
        self.assertEqual(self.dbag[k], val)

    def test_lame_uniqueness(self):
        # this doesn't gaurantee that _genkeys won't ever return a dup, only
        # that it's not doing it right now in the first n iterations
        keys = set()
        while len(keys) < 100:
            k = self.dbag._genkey()
            self.assertNotIn(k, keys)
            keys.add(k)

    def test_set_list(self):
        k,val = 'abc', [1,2,3,4,'xyz']
        self.dbag[k] = val
        self.assertListEqual(self.dbag[k], val)

    def test_set_dict(self):
        k,val = 'abc', {'x': 2, '99':55}
        self.dbag[k] = val
        self.assertDictEqual(self.dbag[k], val)

    def test_set_long_string(self):
        x,y = list(letters), list(letters)
        shuffle(x)
        shuffle(y)
        k,val = 'abc',''.join( [ i+j for i in x for j in y ] )
        self.dbag[k] = val
        self.assertEqual(val, self.dbag[k])

    def test_set_datetime(self):
        k,v = 'dt', datetime.now()
        self.dbag[k] = v
        self.assertEqual(v.isoformat(), self.dbag[k])

    def test_delitem(self):
        k, val = 'abc', 'defghij'
        self.dbag[k] = val
        del self.dbag[k]
        with self.assertRaises(KeyError): self.dbag[k]

        # ensure can't delete nonexistent item
        with self.assertRaises(KeyError): del self.dbag[k]

    def test_when(self):
        # really just want a datetime back
        k, val = 'xyz', 'abcdef'
        self.dbag[k] = val
        self.assertEqual(type(self.dbag.when(k)), datetime)

    def test_iter(self):
        self.dbag['xxx'] = '123'
        self.dbag['aaa'] = 'def'
        keys = [k for k in self.dbag]
        self.assertListEqual(['aaa','xxx'], keys)
        for k in self.dbag:
            self.assertTrue( self.dbag[k] )

    def test_by_created(self):
        self.dbag['xxx'] = '123'
        self.dbag['aaa'] = '123'
        test_d = [('xxx','123'), ('aaa', '123')]
        self.assertListEqual(test_d, [x for x in self.dbag.by_created()])
        self.assertListEqual(
            test_d[::-1],
            [x for x in self.dbag.by_created(desc=True)]
            )

    def test_in(self):
        k, val = '123', 123
        self.dbag[k] = val
        self.assertTrue( k in self.dbag )
        self.assertFalse( 'not there' in self.dbag )

    def test_nondefault_tablename(self):
        self.assertTrue( DataBag(table='something', fpath=':memory:') )


class TestDictBag(unittest.TestCase):

    def setUp(self):
        self.dbag = DictBag( table='testdbag', indexes=(('name', 'age'),) )

    def test_make_index_name(self):
        self.assertEqual(
            self.dbag._make_index_name(('name', 'age')),
            'idx_testdbag_age_name'
            )

    def test_ensure_index(self):
        # test simple index
        self.dbag.ensure_index(('xx',))

        cur = self.dbag._db.cursor()
        cur.execute(
            """
            select count(1) as cnt from sqlite_master
                where type='table' and name='idx_testdbag_xx'
            """
            )
        self.assertEqual( 1, cur.fetchone()['cnt'] )

        cur.execute(
            """
            select count(1) as cnt from sqlite_master
                where type='index' and name='i_idx_testdbag_xx'
            """
            )
        self.assertEqual( 1, cur.fetchone()['cnt'] )

        self.dbag.ensure_index(('yy','xx'))
        cur = self.dbag._db.cursor()
        cur.execute(
            """
            select count(1) as cnt from sqlite_master
                where type='table' and name='idx_testdbag_xx_yy'
            """
            )
        self.assertEqual( 1, cur.fetchone()['cnt'] )

        cur.execute(
            """
            select count(1) as cnt from sqlite_master
                where type='index' and name='i_idx_testdbag_xx_yy'
            """
            )
        self.assertEqual( 1, cur.fetchone()['cnt'] )

    def test_Q_queries(self):
        x = Q('x')
        x = x < 44
        where, params = x.query()
        self.assertEqual( where, ' "x" < ? ')
        self.assertEqual(params, [44])

        x = x >= 33
        where, params = x.query()
        self.assertEqual( where, ' "x" < ? and "x" >= ? ')
        self.assertEqual(params, [44, 33])

        self.assertEqual(
            x._and_ops,
            set( [ (operator.lt, 44), (operator.ge, 33) ] )
        )

        x = x == 'stop'
        where, params = x.query()
        self.assertEqual( where, ' "x" < ? and "x" >= ? and "x" = ? ')
        self.assertEqual(params, [44, 33, 'stop'])

        # test a compound statement
        x = Q('x')
        x = 10 < x < 20
        where, params = x.query()
        self.assertEqual( where, ' "x" > ? and "x" < ? ')
        self.assertEqual(params, [10, 20])

    def test_matching_index(self):
        self.dbag.ensure_index(('yy','xx'))
        self.dbag.ensure_index(('yy',))
        self.dbag.ensure_index(('zz',))

        self.assertEqual(
            ('xx', 'yy'),
            self.dbag._find_matching_index(('xx',))
            )

        self.assertEqual(
            ('zz',),
            self.dbag._find_matching_index(('zz',))
            )

        self.assertEqual(
            ('xx','yy'),
            self.dbag._find_matching_index(('xx',))
            )

        self.assertEqual(
            None,
            self.dbag._find_matching_index(('xx','zz'))
            )

        self.assertEqual(
            None,
            self.dbag._find_matching_index(('not there',))
            )

    def test_only_dicts(self):
        with self.assertRaises( ValueError ):
            self.dbag.add('your mom')

    def test_add_to_index(self):
        self.dbag.ensure_index(('x'))
        self.dbag.ensure_index(('x', 'y'))
        self.dbag.ensure_index(('z', 'y'))

        key = self.dbag.add({'x':23})
        cur = self.dbag._db.cursor()

        idx = self.dbag._make_index_name(('x'))
        cur.execute('''
            select count(1) as cnt from {} where keyf = ?
            '''.format( idx ), (key,))
        self.assertEqual( 1, cur.fetchone()['cnt'] )

        idx = self.dbag._make_index_name(('x', 'y'))
        cur.execute('''
            select count(1) as cnt from {} where x = 23
            '''.format( idx ))
        self.assertEqual( 1, cur.fetchone()['cnt'] )

        idx = self.dbag._make_index_name(('y', 'z'))
        cur.execute('''
            select count(1) as cnt from {} where keyf = ?
            '''.format( idx ), (key,))
        self.assertEqual( 0, cur.fetchone()['cnt'] )

    def test_del_from_index(self):
        self.dbag.ensure_index(('x', 'y'))
        self.dbag.add({'x':22})

    def test_find_kwargs_with_index(self):
        first, second = {'x':10, 'y':99}, {'x':100, 'y':999}
        self.dbag.ensure_index(('x','y'))
        self.dbag.add(first)
        self.dbag.add(second)

        key, found = self.dbag.find_one(x=first['x'])
        self.assertEqual(found['y'], first['y'])

    def test_find_with_Q(self):
        first, second = {'x':10, 'y':99}, {'x':100, 'y':999}
        self.dbag.ensure_index(('x','y'))
        self.dbag.add(first)
        self.dbag.add(second)
        k,ret = next(self.dbag.find( 10 < Q('x') < 101 ))
        self.assertEqual( ret, second )

    def test_not_implemented_search(self):
        with self.assertRaises(NotImplementedError):
            self.dbag.find({'x':{'$zzz':44}})

    def test_search(self):
        first, second = {'x':10, 'y':99}, {'x':100, 'y':999}
        self.dbag.ensure_index(('x','y'))
        self.dbag.add(first)
        self.dbag.add(second)

        k,ret = next(self.dbag.find({'x':10}))
        self.assertEqual( ret, first )

        k,ret = next(self.dbag.find({'x':{'$gt':10}}))
        self.assertEqual( ret, second )

    def test_not_unique(self):
        first, second = {'x':'same', 'y':99}, {'x':'same', 'y':999}
        self.dbag.ensure_index('x')
        # will throw exception if problems
        self.dbag.add(first)
        self.dbag.add(second)


class QuerySetMixin(object):

    # just a set of tests to run, this is used with a real TestCase that sets up
    # the dbag, decides to use indexes or not, then adds the appropriate data

    @classmethod
    def add_data(cls):
        cls.dbag.add( {'x':11} )
        cls.dbag.add( {'x': 50, 'y':'abc'} )
        cls.dbag.add( {'x':99, 'y':'jdk'} )
        cls.dbag.add( {'x': 500, 'y':'abc'} )

    def test_no_key(self):
        key, found = self.dbag.find_one( nokey = 1 )
        assert key is None

    def test_find_keyword(self):
        key, found = self.dbag.find_one(x=11)
        assert found

    def test_find_correct_num_of_matches(self):
        # should be only one match
        assert 1 == len( [ 1 for i in self.dbag.find(x=99) ] )

        # should be two matches
        assert 2 == len( [ 1 for i in self.dbag.find({'x':{'$gt':50}}) ] )

    def test_simple_compound_qdict(self):
        key, found = self.dbag.find_one( { 'x': {'$gt':50, '$lt':100} } )
        assert found

    def test_empty_args(self):
        key, found = self.dbag.find_one()
        assert found['x'] == 500 # should be last one added

    def test_keyword_qdict(self):
        key, found = self.dbag.find_one({'x':{'$gte':99}}, y='abc')
        assert found['x'] == 500

    def test_gt(self):
        assert 2 == len( [ 1 for i in self.dbag.find({'x':{'$gt':50}}) ] )

    def test_gte(self):
        assert 3 == len( [ 1 for i in self.dbag.find({'x':{'$gte':50}}) ] )

    def test_lt(self):
        assert 1 == len( [ 1 for i in self.dbag.find({'x':{'$lt':50}}) ] )

    def test_lte(self):
        assert 2 == len( [ 1 for i in self.dbag.find({'x':{'$lte':50}}) ] )

    def test_ne(self):
        assert 3 == len( [ 1 for i in self.dbag.find({'x':{'$ne':50}}) ] )


class TestQueriesNoIndexes(QuerySetMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbag = DictBag( table='testdbag' )
        cls.add_data()


class TestMyriadOfQueriesWithIndexes(QuerySetMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbag = DictBag( table='testdbag', indexes=(('x', 'y'),) )
        cls.add_data()

