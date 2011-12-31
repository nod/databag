
import sqlite3
import unittest
from datetime import datetime

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
