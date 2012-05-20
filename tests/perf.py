#!/usr/bin/env python


# uber stupid script to measure performance issues.
# databag isn't intended for massive performance, so this doesn't tell us much.


from time import time


# make sure we get our local lib before anything else
import sys, os.path
sys.path = [os.path.abspath(os.path.dirname(__file__)) + '../'] + sys.path

from databag import DataBag


def saves(name, dbag, iters=1000, keynames=True):

    k = 'xyz{}'
    i = 1000
    endi = i + iters
    step = 50 # . every step
    start = time()
    print "starting...", name
    while i < endi:
        if keynames:
            dbag[k.format(i)] = 'letters and numbers'
        else:
            dbag.add('letters and numbers')
        i += 1
    etime = time() - start
    print "   finished."
    print "   {} saves.".format(iters), "elapsed time:{}s".format(time()-start)
    print "   {} saves/sec".format(iters/float(etime))


def main(fpath):

    saves('non-versioned no keys',
        DataBag(fpath, versioned=False), 1000, False)
    saves('non-versioned no keys',
        DataBag(fpath, versioned=False), 10000, False)
    saves('non-versioned', DataBag(fpath, versioned=False), 1000)
    saves('non-versioned', DataBag(fpath, versioned=False), 10000)
    saves('versioned', DataBag(fpath, versioned=True), 1000)
    saves('versioned', DataBag(fpath, versioned=True), 10000)

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print "usage: perf <path to sqlite file>"
        raise SystemExit

    main(sys.argv[1])


