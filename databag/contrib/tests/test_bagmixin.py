
import unittest

try:
    from dictshield.document import Document
    from dictshield.fields import StringField
    dictshield_imported = True
except ImportError:
    # fake these
    dictshield_imported = False

from ..dshield import BagDocument
from ... import Q


if dictshield_imported:

    print "NOTE: running dictshield tests"


    class FakePerson(BagDocument):
        name = StringField()
        city = StringField()


    class BagMixinTest(unittest.TestCase):


        def setUp(self):
            # make joe
            self.fp = FakePerson(name='joe', age=23)
            FakePerson.ensure_index(('name', 'age'))

            # is joe still joe as python(joe)?
            assert self.fp.name == self.fp.to_python()['name']

        def test_instance(self):
            assert isinstance(self.fp, FakePerson)
            assert isinstance(self.fp, Document)
            assert isinstance(self.fp, BagDocument)

        def test_save(self):
            # joe has no _key until after being saved
            assert not self.fp._key
            k = self.fp.save()
            assert self.fp._key == k

            # the db table name comes from __class__ but that gets munged so
            # let's make sure it was set appropriately
            assert self.fp._dbag._bag == 'FakePerson'

        def test_find(self):
            self.fp.save()
            fp = FakePerson.find(name=self.fp.name).next()
            assert fp.name == self.fp.name

        def test_fetch(self):
            k = self.fp.save()
            o = FakePerson.fetch(k)
            print o._key, self.fp._key
            assert o._key == self.fp._key

        def test_fetch_not_there(self):
            with self.assertRaises(KeyError):
                FakePerson.fetch('nobody home')


