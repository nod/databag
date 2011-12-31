
import sqlite3
import unittest
from datetime import datetime
from random import shuffle
from string import letters


from databag import DataBag

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
        sql = '''insert into {} (keyf, data) values ('xx','zzz')
              '''.format( self.dbag._bag )
        cur.execute(sql)
        with self.assertRaises(sqlite3.IntegrityError): cur.execute(sql)

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
        print self.dbag.when(k)
        self.assertEqual(type(self.dbag.when(k)), datetime)

    def test_iter(self):
        self.dbag['xxx'] = '123'
        self.dbag['aaa'] = 'def'
        keys = [k for k in self.dbag]
        self.assertListEqual(['aaa','xxx'], keys)

        for k in self.dbag:
            self.assertTrue( self.dbag[k] )

    def test_in(self):
        k,val = '123', 123
        self.dbag[k] = val
        self.assertTrue( k in self.dbag )
        self.assertFalse( 'not there' in self.dbag )
