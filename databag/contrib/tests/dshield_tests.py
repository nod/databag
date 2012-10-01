
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

        class _Meta(BagDocument._Meta):
            indexes = ( ('name', 'city'), )


    class BagMixinTest(unittest.TestCase):

        def setUp(self):
            # clear out the db between tests
            BagDocument._dbag = None

            # make joe
            self.fp = FakePerson(name='joe', age=23)

        def test_instance(self):
            # is joe still joe as python(joe)?
            assert self.fp.name == self.fp.to_python()['name']

            assert isinstance(self.fp, FakePerson)
            assert isinstance(self.fp, Document)
            assert isinstance(self.fp, BagDocument)

        def test_save(self):
            # joe has no _key until after being saved
            assert not self.fp._key
            k = self.fp.save()
            assert self.fp._key == k

            new_k = 'himom'
            self.fp.save(new_k)
            assert self.fp._key == new_k

        def test_bag_name(self):
            # the db table name comes from __class__ but that gets munged so
            # let's make sure it was set appropriately
            assert self.fp._dbag._table == 'FakePerson'

        def test_find(self):
            self.fp.save()
            fp = FakePerson.find(name=self.fp.name).next()
            assert fp.name == self.fp.name

        def test_fetch(self):
            k = self.fp.save()
            o = FakePerson.fetch(k)
            assert o._key == self.fp._key

        def test_fetch_not_there(self):
            with self.assertRaises(KeyError):
                FakePerson.fetch('nobody home')

        def test_find_one(self):
            k = self.fp.save()
            fp = FakePerson.find_one(name=self.fp.name)
            assert fp._key == self.fp._key

            with self.assertRaises(KeyError):
                FakePerson.find_one(name='nowhere man')

        def test_exists(self):
            k = self.fp.save()

            # check key
            assert FakePerson.exists(k)

            # check for object
            assert FakePerson.exists(self.fp)

        def test_all(self):
            self.fp.save()
            count = 0
            for x in FakePerson.all():
                count += 1
            assert count == 1

            count = 0
            FakePerson(name='2nd').save()
            for x in FakePerson.all():
                print x.name
                count += 1
            assert count == 2


