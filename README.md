# PUT YOUR DATA IN A BAG

Pretty simple library for just splatting stuff to disk and getting it back out
with minimal fuss.

## features

* Easy to use and quite efficient at accessing relatively large number of items
* Requires no other libs, everything is python batteries included.
* Built on top of sqlite3 so it's fast and stable (which is included in Python
  stdlib)
* Easy to use - just create one and use it like a dictionary. Most dict methods
  supported
* Pretty well tested
* Ideal for running on small vm instances.  Doesn't require any other daemon to
  provide data access
* Core code is about 100 lines - very easy to understand.
* Automatically compresses data with bz2 in cases that benefit from it
* You can always query the data with native sqlite3 libs from other languages
  if you need to.  It's just strings in the database.
* Since the underlying datafile is sqlite3, multiple processes can work with
  the same file (multiple read, write locks, etc)
* Every object gets a ts object attached to it for convenience when it's saved.
  This is accessed via `bag.when('key')`

## examples

```python
>>> from databag import DataBag
>>> bag = DataBag() # will store sqlite db in memory
>>> bag['xyz'] = 'some string' # will save in the db
>>> s = bag['xyz'] # retrieves from db
>>> s
'some string'
>>> 'xyz' in bag # True
True
>>> bag['abc'] = {'x':22, 'y':{'a':'blah'}} # works
>>> bag['abc']
{u'y': {u'a': u'blah'}, u'x': 22}
>>> [k for k in bag]
['abc', 'xyz']
>>> bag.when('xyz')
datetime.datetime(2011, 12, 31, 2, 45, 47, 187621)
>>> del bag['xyz']
>>> 'xyz' in bag
False
>>> meh = DataBag(bag='other') # set name of storage table
```

## limitations

* although a lot of the basic data types in python are supported for the values
  (lists, dictionaries, tuples, ints, strings)... datetime objects can be saved
  fine but they come out of the bag as an iso format string of the original
  datetime.
* when saving a dictionary, the keys must be a string in the dictionary.  If
  they are not, they will be when coming back from the bag



