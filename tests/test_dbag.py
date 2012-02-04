
import sqlite3
import unittest
from datetime import datetime
from random import shuffle
from string import letters

# make sure we get our local lib before anything else
import sys, os.path
sys.path = [os.path.abspath(os.path.dirname(__file__)) + '../'] + sys.path


from databag import DataBag, ObjectDict

class TestObjDict(unittest.TestCase):

    def test_objdict(self):
        """
        super trivial lame test
        """
        d = {'x': 'xyz'}
        o = ObjectDict(d)
        self.assertEqual( d['x'], o.x )


class TestDataBag(unittest.TestCase):

    def setUp(self):
        self.dbag = DataBag()

    def test_ensure_table(self):
        cur = self.dbag._db.cursor()

        cur.execute(
            '''SELECT name FROM sqlite_master WHERE type='table' AND name=?''',
            (self.dbag._bag,)
            )
        self.assertEqual(cur.fetchone()['name'], self.dbag._bag)

        # check the unique index
        sql = '''insert into {} (keyf, data, ver) values ('xx','zzz', 0)
              '''.format( self.dbag._bag )
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
        for x in xrange(1,10):
            d_v[key] = 'again'*x

    def test_set_get(self):
        k,val = 'zz', 'more stuff'
        self.dbag[k] = val
        self.assertEqual(val, self.dbag[k])

    def test_set_int(self):
        k,val = 'abc', 555
        self.dbag[k] = val
        self.assertEqual(self.dbag[k], val)

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

        # test can't delete nonexistent item
        with self.assertRaises(KeyError): del self.dbag[k]

    def test_when(self):
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
        k,val = '123', 123
        self.dbag[k] = val
        self.assertTrue( k in self.dbag )
        self.assertFalse( 'not there' in self.dbag )

    def test_nondefault_tablename(self):
        self.assertTrue( DataBag(fpath=':memory:', bag='something') )

