#!/usr/bin/env python

# incredibly stupid script to measure performance issues.
# databag isn't intended for massive performance, so this doesn't tell us much.

from time import time

from databag import DataBag


def saves(name, dbag, iters=1000, keynames=True):
    print(f"test: {name}  keynames={keynames} ... iters={iters} ")
    k = 'xyz{}'
    i = 1000
    endi = i + iters
    start = time()
    while i < endi:
        if keynames:
            dbag[k.format(i)] = 'letters and numbers' + str(i)
        else:
            dbag.add('letters and numbers')
        i += 1
    etime = time() - start
    print(f"  - total:{etime}s  per100:{etime/iters*100}")


def main(fpath):
    saves('non-versioned no keys',
        DataBag('perfy', fpath, versioned=False), 1000, False)
    saves('non-versioned no keys',
        DataBag('perfy', fpath, versioned=False), 10000, False)
    saves('non-versioned', DataBag('perfy', fpath, versioned=False), 1000)
    saves('non-versioned', DataBag('perfy', fpath, versioned=False), 10000)
    saves('versioned', DataBag('perfy', fpath, versioned=True), 1000)
    saves('versioned', DataBag('perfy', fpath, versioned=True), 10000)


if __name__ == '__main__':

    import sys
    if len(sys.argv) != 2:
        raise SystemExit

    main(sys.argv[1])


