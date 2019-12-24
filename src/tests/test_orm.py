
import unittest
from datetime import datetime

from databag import DictBag, Q
from databag.orm.model import Field, IntField, Model, set_db_path


class Faker(Model):
    name = Field(str)
    age = IntField()


class TestORM(unittest.TestCase):

    def setUp(self):
        self.dbag = DictBag(Faker.table_name())
        Faker.set_db(self.dbag)

    def test_makegetset(self):
        f0 = Faker(name="joe", age=10).save()
        t0 = self.dbag[f0.key]
        self.assertEqual( f0.name, t0['name'] )

    def test_query(self):
        f0 = Faker(name="joe", age=10).save()
        f1 = Faker(name="sue", age=11).save()
        f2 = Faker(name="max", age=8).save()
        big_kids = list(Faker.find(Q('age')>8))
        self.assertEqual(len(big_kids), 2)
        k,just_max = Faker.find_one(name='max')
        self.assertEqual(just_max.name, 'max')

    def test_grab(self):
        f0 = Faker(name="joe", age=10).save()
        t0 = Faker.grab(f0.key)
        self.assertEqual(t0.key, f0.key)

    def test_field_type(self):
        f0 = Faker(name="mary", age="9").save()
        self.assertEqual(f0.age, 9)

    def test_created_ts(self):
        f0 = Faker(name="bill", age=44)
        self.assertTrue(isinstance(f0._created_ts, datetime))


