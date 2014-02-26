
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
    i am now using the joblib solution. this looks good


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


locking:
    havnt quite figured it out yet, but almost there
    lockfile based locking makes for a good default;
    but it would be nice to be able to disable when NFS compatibility is not needed
"""



import os

import tempfile
from shelve2 import Shelve, process_key, encode
from time import clock, sleep, time

import threading


import numpy as np
import inspect
from serialization import as_deterministic


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
    abstract base class of a cache object which gracefully handles large arbitrary key objects

    operation and environment methods must be overloaded
    """
    def __init__(
            self,
            identifier          = None,     #name under which the cache file is stored. defaults to modulename.classname
            environment         = None,     #object containing information regarding the dependencies of the cached operation
            operation           = None,     #function to be cached. note that the order of arguments is significant for key reuse
            validate            = False,    #validation mode. if enabled, all cache retrievals are checked against a recomputed function call.
            deferred_timeout    = 30,       #time to wait before a deferred object is considered obsolete. compilation may take a long time; that said, it may also crash your process...
            lock_timeout        = 1,        #time to wait before a lock is considered obsolete. the lock is needed for pure db transactions only; this makes once second a long time
            environment_clear   = True,     #clear the cache upon connection with a novel environment key
            connect_clear       = False     #clear the cache upon every connection
            ):
        """
        if environment_clear is set to true, the cache is cleared
        """
        if identifier: self.identifier = identifier
        if operation: self.operation = operation
        #add some essentials to the environment
        import platform
        globalenv               = platform.architecture(), platform.python_version()
        funcenv                 = inspect.getargspec(self.operation), inspect.getsource(self.operation)
        self.environment        = globalenv, funcenv, (environment if environment else self.environment())
        estr, ehash             = process_key(self.environment)

        self.validate           = validate
        self.lock_timeout       = lock_timeout
        self.deferred_timeout   = deferred_timeout

        self.filename           = os.path.join(cachepath, self.identifier)
        self.shelve             = Shelve(self.filename, autocommit = True)
        self.lock               = threading.Lock()
        self.lock_file          = lockfile.MkdirLockFile(self.filename, timeout = lock_timeout)

        with self.lock_file:
            if connect_clear:
                #this isnt right; we are now invalidating the precomputed envrowid of other processes...
                self.shelve.clear()           #write lock

            #write environment key to database and obtain its unique rowid
            try:
                self.envrowid = self.shelve.getrowid(self.environment, estr, ehash)
            except:
                #connect to the db with a novel environment; probably wont change back again
                if environment_clear:
                    self.shelve.clear()         #write lock
                self.shelve.setitem(self.environment, None, estr, ehash)
                self.envrowid = self.shelve.getrowid(self.environment, estr, ehash)




    def __call__(self, *args, **kwargs):
        """
        look up a hierachical key object
        fill in the missing parts, and perform the computation at the leaf if so required
        """
        #kwargs are last subkey?
        hkey = args + ((kwargs,) if kwargs else ())
        #preprocess subkeys. this minimizes time spent in locked state
        hkey = map(as_deterministic, hkey)

        while True:
            try:
                with self.lock, self.lock_file:
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
                        #check if recomputed value is identical under deterministic serialization
                        newvalue = self.operation(*args, **kwargs)
                        assert(as_deterministic(value)==as_deterministic(newvalue))

                    #yes! hitting this return is what we are doing this all for!
                    return value

            except:
                #lock for the writing branch. multiprocess does not benefit here, but so be it.
                #worst case we make multiple insertions into db, but this should do no harm for behavior

                if self.lock.locked() or self.lock_file.is_locked():
                    #if lock not available, better to go back to waiting for a deferred to appear
                    sleep(0.001)
                else:
                    with self.lock_file:
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

                    #dont need lock while doing expensive things
                    value = self.operation(*args, **kwargs)

                    with self.lock_file:
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




##class CacheDecorator(object):
##    def __init__(
##            self,
##            operation,              #function to be cached
##            identifier  = None,     #if none, the module and function name of operation are used to generate a representative identifier
##            environment = None,     #to specify source dependencies of the cached operation
##            **kwargs                #additional kwargs will be passed to the cache object
##            ):
##        """
##        provide a function with caching behavior in a minimally intrusive manner
##        """
##        identifier  = identifier  if identifier  else inspect.getmodule(operation).__name__ + '_' + operation.__name__
##        environment = environment if environment else True
##        self.cache = AbstractCache(identifier, environment, operation, **kwargs)
##
##    def __call__(self, *args, **kwargs):
##        return self.cache(*args, **kwargs)

def CacheDecorator(
        identifier  = None,     #if none, the module and function name of operation are used to generate a representative identifier
        environment = None,     #to specify source dependencies of the cached operation
        **kwargs                #additional kwargs will be passed to the cache object
        ):
    """
    wrap a function with caching behavior
    """
    def wrap(operation):
        cache = AbstractCache(
            identifier  = identifier  if identifier  else inspect.getmodule(operation).__name__ + '_' + operation.__name__,
            environment = environment if environment else True,
            operation   = operation,
            **kwargs)
        def inner(*args, **kwargs):
            return cache(*args, **kwargs)
        return inner
    return wrap
cached = CacheDecorator     #an alias
memoize = CacheDecorator    #prebind some arguments relating to in-memory storage?



"""
client code starts here;
from pycache import cached
"""

import numpy as np



if False:
    @cached(connect_clear=True)
    def compile(source, templates):
        print 'compiling'
        sleep(1)
        return source.format(**templates)

    print compile('const {dtype} = {value};', dict(dtype='int',value=3))
    print compile('const {dtype} = {value};', dict(dtype='int',value=3))
    quit()





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


cache = CompilationCache(connect_clear=True)



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