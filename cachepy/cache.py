
"""
cross platform thread and process safe caching of python functions to disk

the caching mechanism here is designed to free specific cache implemntations
from low level database drudgery, and worries about thread/process issues, rather than performance.
presumably, the data being cached (compilation output, and so on), is data with a high compute intensity

yet the solution is very scalable, and the time complexity of all solutions used should be good
hashing is used only as a way to perform efficient database lookup with minimal memory footprint
the design is such that hash collisions cannot compromise correctness of the caching,
since all key information used is also stored on disk

subclassing Cache.operation allow implementation of the cached operation
this could be a compilation, a complex expression graph optimization,
or what have you


every cache has an environment description associated with it
this environment should encapsulate all machine state relavant to the
behavior of the cached function, which is not explicitly part of its input arguments
in the simplest case, it could be a compiler version number,
but it may just as well be the entire source code of a compiler


the intended use case is for various stages of compilation caching
as such, absolute performance is not our main concern in this design
if the process you intend to cache is slow relative to a pickling of the datastructure on which it acts,
you probably shouldnt be caching it in the first place. but if this is a concern,
pickling a string trivial; so if you feel you can do better serialization, you are welcome to






validation mode:
    all cached calls are both read from the database as well as recomputed, and then checked for correctness
    this is useful to check the correctness of this code, as well as checking the correctness of ones'
    environment implementation



note on the determinism of serialization:
    pickling of dicts is not deterministic, in the sense that the outcome may depend on insertion order in the dict
    and there are more gotchas along these lines.
    for local caching, this isnt such a problem; worst case we do some extra compilation
    but for pycc type caching, this is of course very important

    the serialization module contains code to preprocess key objects to a deterministic form
    all container types are recursively treated, including the attrs of user defined types
    im not sure how robust this is; review here would be greatly appreciated
    not that in its present form, cyclic references and reference equality are not respected


joblib integration:
    quite some overlap. steal its deterministic pickling code?
joblib missing features:
    exact key storage
    locking mechanism
    deferred mechanism
    environment mechanism


ideally, create a variety of disk based key-value stores.
different scenarios will call for different shelve designs:
    read/write
    exact/inexact keys
    different locking strategies
    local/server based

"""

##import joblib
##joblib.hash



import os

import tempfile
from shelve2 import Shelve, process_key, encode
from time import clock, sleep, time

import threading
import collections


import numpy as np



import lockfile.mkdirlockfile as lockfile

lock_thread = True
lock_file = True


temppath = tempfile.gettempdir()
cachepath = os.path.join(temppath, 'cachepy')
try:
    os.mkdir(cachepath)
except:
    pass

import datetime


class Deferred(object):
    """
    timestamped deferred token.
    placed in database to inform other threads/processes an entry is under construction
    this prevents unnecessary duplication of work in the case of multiple threads/processes
    demanding the same cached value at similar times
    """
    def __init__(self):
        self.stamp = time()
    def expired(self, timeout):
        dt = time() - self.stamp
        return dt > timeout or dt < 0
    def __str__(self):
        return 'Deferred Value: ' + str(self.stamp)
    def __repr__(self):
        return str(self)

class Partial(object):
    """
    token denoting a partial key insertion into the shelve
    rowid is the unique rowid value where the preceeding part of the key can be found
    """
    def __init__(self, rowid):
        self.rowid = rowid
    def __str__(self):
        return 'Partial Key: '+str(self.rowid)
    def __repr__(self):
        return str(self)





class AbstractCache(object):
    """
    cache object which gracefully handles large arbitrary key objects

    operation and environment methods must be overloaded
    """
    def __init__(self, identifier=None, environment=None, operation=None, validate=False, timeout = 10):
        if identifier: self.identifier = identifier
        if operation: self.operation = operation
        self.environment = environment if environment else self.environment()
        self.validate = validate

        self.filename           = os.path.join(cachepath, self.identifier)
        self.lock = threading.Lock()
        self.lock_file = lockfile.MkdirLockFile(self.filename, timeout = timeout)

        self.shelve             = Shelve(self.filename, autocommit = True)
        self.shelve.clear()   #for debugging


        #write environment key to database and obtain its unique rowid
        estr, ehash = process_key(self.environment)
        self.shelve.setitem(self.environment, None, estr, ehash)
        self.envrowid = self.shelve.getrowid(self.environment, estr, ehash)




    def __call__(self, *args, **kwargs):
        """
        look up a hierachical key object
        fill in the missing parts, and perform the computation at the leaf if so required
        """
        hkey = args.append(kwargs)
        while True:

                try:
                    #hierarchical key lookup; first key is prebound environment key
                    previouskey = Partial(self.envrowid)
                    for ikey, subkey in enumerate(hkey[:-1]):
                        partialkey = previouskey, subkey
                        rowid = self.shelve.getrowid(partialkey, *process_key(partialkey))  #read lock?
                        previouskey = Partial(rowid)
                    #leaf iteration
                    ikey = len(hkey)-1
                    leafkey = previouskey, hkey[-1]
                    value = self.shelve[leafkey]                                            #read lock?

                    if isinstance(value, Deferred):
                        if value.expired(self.deferred_timeout):
                            raise Exception()
                        sleep(0.01)
                    else:
                        if self.validate:
                            newvalue = self.operation(*args, **kwargs)
                            assert(Pickle.dumps(value)==Pickle.dumps(newvalue))
                        return value

                except:
                    #lock for the writing branch. multiprocess does not benefit here, but so be it.
                    #worst case we make multiple insertions into db, but this should do no harm for behavior

                    if self.lock.locked():
                        #if lock not available, better to go back to waiting for a deferred to appear
                        sleep(0.01)
                    else:
                        with self.lock:
                            #hierarchical key insertion
                            for subkey in hkey[ikey:-1]:
                                partialkey = previouskey, subkey
                                kstr, khash = process_key(partialkey)
                                self.shelve.setitem(partialkey, None, kstr, khash)      #wite lock
                                rowid = self.shelve.getrowid(partialkey, kstr, khash)   #read lock
                                previouskey = Partial(rowid)
                            #insert leaf node
                            leafkey = previouskey, hkey[-1]
                            kstr, khash = process_key(leafkey)
                            self.shelve.setitem(leafkey, Deferred(), kstr, khash)       #write lock
                            value = self.operation(*args, **kwargs)
                            self.shelve.setitem(leafkey, value     , kstr, khash)       #write lock
                            return value




    def operation(self, input):
        """
        implements the cached operation; to be invoked upon a cache miss
        input is a picklable python object
        the returned output should be a pickalble python object as well
        """
        raise NotImplementedError()
    def environment(self):
        """
        returns a pickeable object describing the environment of the cached operation,
        or a description of the state of your computer which may influence the relation
        between the input and output of Cache.cached
        """
        raise NotImplementedError()


"""
client code starts here;
from pycache import Cache
"""



import numpy as np

class CompilationCache(AbstractCache):
    """
    subclass implements the actual cached operation
    """
    identifier = 'numba_compilation'

    def operation(self, source, templates):

        print 'compiling'
        sleep(1)
        return source.format(**templates)


    def environment(self):
        version='3.4'
        compiler='llvm'
        return version, compiler


cache = CompilationCache()



##class GraphOptimizationCache(AbstractCache):
##    identifier = 'theano_graph'
##
##    def cached(self, source):
##        n, s = source
##
##        print 'compiling'
##        sleep(3)
##        q = np.array(list(s*n))
##        return np.sort(q).tostring()
##
##
##    def environment(self):
##        import numba
##        version = numba.__version__
##
##        import inspect
##        files = [numba.Function, numba.Accessor]    #some random files....
##        informal_version = [inspect.getsource(file) for file in files]
##
##        return version, informal_version, 999



def worker(arg):
    value = cache(*arg)
    return value


if __name__=='__main__':

    #test compiling the same function many times, or compilaing different functions concurrently

    args = [('const {dtype} = {value};', dict(dtype='int',value=3))]*10
##    args = [('const {dtype} = {value};', dict(dtype='int',value=i)) for i in range(10)]

    #run multiple jobs concurrent as either processes or threads
    threading=False
    if threading:
        import multiprocessing.dummy as multiprocessing
    else:
        import multiprocessing

    #sleep(0.1)

    pool = multiprocessing.Pool(4)
    for r in pool.imap(worker,  args):
        print r

    for k in cache.shelve.keys():
        print k
##    print cache[(3,'a')]