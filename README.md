# PUT YOUR DATA IN A BAG

Pretty simple library for just splatting stuff to disk and getting it back out
with minimal fuss.

## features

* Easy to use and way more efficient than pickling
* requires no other libs.  everything is native python
* built on top of sqlite3 so it's fast and stable
* easy to use.  just create one and use it like a dictionary. most dict methods supported
* mostly well tested
* core code is about 100 lines of code. very easy to understand.
* automatically compresses data with bz2 in cases that benefit from it
* will try to jsonify data going into it if it's not already a string
* You can always query the data with native sqlite3 libs from other languages
  if you need to.  It's just strings in the database.

## example's probably the easiest way to describe it

```python
>>> from databag import DataBag
>>> bag = DataBag() # will store sqlite db in memory
>>> bag['xyz'] = 'some string' # will save in the db
>>> s = bag['xyz'] # retrieves from db
>>> s
'some string'
>>> 'xyz' in bag # True
True
>>>
>>> bag['abc'] = {'x':22, 'y':{'a':'blah'}} # works
>>> bag['abc']
{u'y': {u'a': u'blah'}, u'x': 22}
```

## limitations

* although a lot of the basic data types in python are supported for the values
  (lists, dictionaries, tuples, ints, strings)... datetime objects can be saved
  fine but they come out of the bag as an iso format string of the original
  datetime.
* when saving a dictionary, the keys must be a string in the dictionary.  If
  they are not, they will be when coming back from the bag



