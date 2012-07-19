
from dictshield.document import Document

from ..databag import DictBag

DBPATH = None


class BagDocument(Document):

    _key = None
    _dbag = None

    def __init__(self, *a, **ka):
        super(BagDocument, self).__init__(*a, **ka)
        if not BagDocument._dbag:

            # due to metaclassery, the __class_ get smunged so let's give it a
            # hint as to the type of document we're really creating
            table_name = self._class_name.split('.')[-1]

            BagDocument._dbag = DictBag(
                fpath=DBPATH,
                bag=table_name
                )

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


