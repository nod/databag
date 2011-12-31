# simple object wrapper around sqlite tables

it's very simple but it's pretty well tested


## example

```
>>> from databag import DataBag
>>> bag = DataBag() # will store sqlite db in memory
>>> bag['xyz'] = 'some string' # will save in the db
>>> s = bag['xyz'] # retreives from db
>>>
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



