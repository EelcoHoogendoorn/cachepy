
"""
locking module

mkdir based lockfile locking seems the way to go for our application

it isnt the most efficient solution, but it is the most portable and robust one
given the small number of transactions that we are dealing with in a compilation cache,
the performance of atomic file system based operation should be fine

however, support for an arbitrary number of concurrent reads would be nice,
and it does not appear there is an existing lockfile package out there which takes care of this.
the dynamic to enforce is that a write lock can only be acquired if no other locks are present
and a read lock can only be acquired if no write lock is present

that said, this may be premature optimization
just start with an existing read/write agnostic mutex for now
otoh; the reason we bother with these lockfiles is nfs support
if 200 cluster nodes want to read from the same file, itd be nice
if we can handle more than 10 transactions per second...

also, lock timeout functionality is a must


https://github.com/smontanaro/pylockfile
https://github.com/dmfrey/FileLock

http://twistedmatrix.com/trac/browser/trunk/twisted/python/lockfile.py
https://bitbucket.org/pchambon/python-rock-solid-tools/src/74e361d85015?at=default
"""

import lockfile.mkdirlockfile as LockFile
