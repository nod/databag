# PUT YOUR DATA IN A BAG

pretty simple library for just splatting stuff into an sqlite table and getting
it back out with minimal fuss


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

* anything assigned to the bag that's not a string will get json encoded going
  in and coming out
* all data is bz2'd when going into the sqlite3 database to save space


## issues

* when saving a dictionary, the keys must be a string in the dictionary.  If
  they are not, they will be when coming back from the bag



