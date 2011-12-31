# PUT YOUR DATA IN A BAG

pretty simple library for just splatting stuff into an sqlite table and getting
it back out with minimal fuss

## features

* brain-dead easy to use and way better than pickling
* requires no other libs.  everything is native python
* built on top of sqlite3 so it's fast and stable
* easy to use.  just create one and use it like a dictionary. most dict methods supported
* mostly well tested
* core code is about 100 lines of code. very easy to understand.
* automatically compresses data with bz2 in cases that benefit from it
* will try to jsonify data going into it if it's not already a string


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

## issues

* when saving a dictionary, the keys must be a string in the dictionary.  If
  they are not, they will be when coming back from the bag



