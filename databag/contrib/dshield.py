
from dictshield.document import Document

from .. import DictBag, Q

DBPATH = None


class BagDocument(Document):
    """
    a child class for dictshield.Document that provides additional methods for
    storing dictshield instances in a databag.

    ```
    >>> from databag.contrib.dshield import BagDocument
    >>> import dictshield.fields as df
    >>> class Person(BagDocument):
    ...     name = df.StringField()
    >>> p = Person(name='joe')
    >>> p.save()
    'nQS0FfpSPkgEcLJgYxx2wN'
    >>> class Person(BagDocument):
    ...     name = df.StringField()
    ...     class _Meta(object):
    ...         indexes = ('name',)
    ```
    """


    _key = None
    _dbag = None

    class _Meta(object):
        """
        defining this is optional but can be used to automatically ensure
        indexes on the databag in a consistent manner
        """
        indexes = tuple()
        dbpath = DBPATH

    def __init__(self, *a, **ka):
        super(BagDocument, self).__init__(*a, **ka)
        type(self).setup_db()

    @classmethod
    def setup_db(cls):
        """
        instantiates the database connection as needed. Can be called repeatedly
        but should definitely be called prior to any queries.
        """
        if not cls._dbag:

            # due to metaclassery, the __class_ get smunged so let's give it a
            # hint as to the type of document we're really creating
            table_name = cls._class_name.split('.')[-1]

            BagDocument._dbag = DictBag(
                fpath=cls._Meta.dbpath,
                bag=table_name
                )

            for i in cls._Meta.indexes:
                if isinstance(i, basestring):
                    # convenience for just one col in the index
                    BagDocument._dbag.ensure_index((i,))
                else:
                    BagDocument._dbag.ensure_index(i)


    def save(self):
        """
        saves a BagDocument in the bag
        """
        if self._key:
            self._dbag[self._key] = self.to_python()
        else:
            self._key = self._dbag.add(self.to_python())
        return self._key

    @classmethod
    def find(cls, *a, **ka):
        """
        accepts a query and returns an iterator of instances of the BagDocument
        """
        for k,d in cls._dbag.find(*a, **ka):
            yield cls(_key=k, **d)

    @classmethod
    def ensure_index(cls, *a, **ka):
        """
        queries require an index to be built on the data.  this is a simple
        passthru to dictbag's ensure_index method
        """
        cls._dbag.ensure_index(*a, **ka)

    @classmethod
    def fetch(cls, key):
        """
        return just one BagDocument by key
        """
        return cls(_key=key, **(cls._dbag[key]))

    @classmethod
    def find_one(cls, *a, **ka):
        """
        return just one BagDocument by key
        """
        try:
            return cls.find(*a, **ka).next()
        except StopIteration:
            raise KeyError

    @classmethod
    def all(cls):
        """
        iterates through each item in the dbag and returns object
        """
        for x in cls._dbag:
            yield cls(**cls._dbag[x])


