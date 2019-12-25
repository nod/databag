
from databag import DictBag, Q
from .field import Field, IntField, DateTimeField, StrField


class MissingDBConnection(Exception): pass


class DBConnection:
    def __init__(self):
        self._dbp = None

    def set_db_path(self, dbp):
        DBConnection._dbp = dbp

    @property
    def dbpath(self):
        if DBConnection._dbp is None:
            raise MissingDBConnection("must instantiate dbconnection first")
        return DBConnection._dbp

dbconn = DBConnection()

def set_db_path(dbp):
    dbconn.set_db_path(dbp)


class Model:
    _created_ts = DateTimeField()
    _key = None
    __fields = None
    __db = None
    __values = None

    def __init__(self, key=None, **ka):
        self._update_fields()
        self.__values = {}
        self._key = key
        for k,a in ka.items():
            setattr(self, k, a)

    def __setitem__(self, k, val):
        self.__values[k] = val

    def __getitem__(self, k):
        # instance[k] should always return the raw value I know about
        # we assume other access methods have run through the field's default
        # setters by now
        return self.__values.get(k)

    @classmethod
    def _update_fields(cls):
        """ update the fields dict """
        cls.__fields = {}
        for k in dir(cls):
            fld = getattr(cls, k)
            if not isinstance(fld, Field): continue
            fld.set_field_name(k)
            cls.__fields[k] = fld

    def to_d(self):
        return { k:getattr(self, k) for k in self.__fields }

    @classmethod
    def grab(cls, key):
        obj = cls.from_d(cls._db()[key])
        obj._key = key
        return obj

    @property
    def key(self):
        return self._key

    @staticmethod
    def key_field():
        return 'id'

    @classmethod
    def table_name(cls):
        return cls.__name__.lower()

    @classmethod
    def _db(cls):
        if cls.__db is None:
            cls.__db = DictBag(cls.table_name(), dbconn.dbpath)
        return cls.__db

    @classmethod
    def set_db(cls, db):
        """
        if you use the set_db_path method in this library then calling set_db on
        each model is unnecessary
        """
        cls.__db = db

    @classmethod
    def from_d(cls, some_dict):
        key = some_dict.get(cls.key_field(), None)
        obj = cls(key)
        # be generous, add all of dict to model
        for k,f in cls.__fields.items():
        # for k,v in some_dict.items():
            val = some_dict.get(k)
            if val is not None:
                setattr(obj,k,f._from_json(val))
        return obj

    def save(self):
        # are we a new object, or updating?
        if self.key: self._db[self.key] = self.to_d()
        else:
            self._key = self._db().add(self.to_d())
        return self

    @classmethod
    def find(cls, *qdicts, **ka):
        for k,d in cls._db().find(*qdicts, **ka):
            yield k,cls.from_d(d)

    @classmethod
    def find_one(cls, *qdicts, **ka):
        return next(cls.find(*qdicts, **ka))

    @classmethod
    def iter(cls):
        for k,d in cls._db().find():
            yield k, cls.from_d(d)



