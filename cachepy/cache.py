
"""
cross platform thread and process safe caching of python functions to disk

the caching mechanism here is primarily geared towards freeing specific cache implemntations
from low level database drudgery, and worried about thread/process issues rather than performance.
presumably, the data being cached (compilation output, and so on), is data with a high value density

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


three layered architecture:
    shelve2:
        efficient thread/process safe object-to-object mapping
    generic caching layer:
        environment caching, pycc integration, deferred tokens, NFS safety?
    project specific code deriving from abstract cache object:
        external compilation caching
        graph optimization caching
        or other arbitrarily fine-grained processes, each implementing an appropriate environment overload


is this achitecture sufficient?
the price for the simplicity of this architecture is a slight wastefulness;
the source code of a function with different overloads will be stored in its entirely for each overload, for instance
but until proven otherwise, i think this should be considered premature optimization
trading kilobytes of harddisk space for seconds of runtime performance is the nature of the game we are playing
your milage may vary, but i have a lot more kilobytes of drive space than i have seconds of patience
one way to optimize this might be to use a custom serializer, where subobjects marked as being suspected of being highly repetitive
get placed in a seperate table, and are subsequently refered to by their unique rowid in other tables



two possible improvements:
    implemented hierarchical keys:
        first iterable of key denoted hierarchy
        look up keys in stages
        look up first part of key in dict
        if last part, return value
        if not last part, return rowid
        next lookup

    include optional inexact information as well? like a hash of the standard library?

    environment info can be folded into hierarchy
    also allows sqlite to handle atomicity
    also a bit more elegant; the seperation between state and arguments is nothing but an ill defined convention anyway
    not quite; it is nice to unify on the bd level, but conceptuall environment and input are somewhat seperate things,
    in the way they manifest in the code. input is everything which is passed to the function to be cached,
    environment everything else.

possible example structure of the various layers a key into the db:
    environment key: all relevant source files that affect the process, but which are static over the
    module key: collection of input files to be compiled, and their dependencies
    function name: the function to be bound
    template/jit compile time arguments: datastructure

the environment key is only checked and bound at the time the cache is created


validation mode:
    all cached calls are both read from the database as well as recomputed, and then checked for correctness
    this is useful to check the correctness of this code, as well as checking the correctness of ones'
    environment implementation


optionally use python lockfile for NFS compatibility
just wrap the write accesses in a lock
can we read while another NFS process is writing?
ask around
dont maintain the lock while running the computation;
neither threadlock or filelock
if db is locked for writing, go back to looking for a deferred
lockfile based locking is generally frowned upon performance wise
but given the very low volume of writes to this particular cache,
it is a perfectly fine solution actually
make lockfile based locking optional though; it should not be necessary in most use cases
"""



import os

import tempfile
from shelve2 import Shelve, process_key
from time import clock, sleep, time

import threading
import collections


import numpy as np



import lockfile.mkdirlockfile as lockfile

lock_thread = True
lock_file = True


temppath = tempfile.gettempdir()
cachepath = os.path.join(temppath, 'pycache')
try:
    os.mkdir(cachepath)
except:
    pass

import datetime
datetime.datetime

class Deferred(object):
    """timestamped deferred token"""
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
    """token denoting a partial key insertion into the shelve"""
    def __init__(self, rowid):
        self.rowid = rowid
    def __str__(self):
        return 'Partial Key: '+str(self.rowid)
    def __repr__(self):
        return str(self)


##class Cache(object):
##    """
##    thread and process safe disk based cache,
##    mapping pickleable python objects to pickleable python objects
##    how to handle environment?
##    store environment as seperate pickle? or as a single table?
##    also this environment mechanism needs to be thread and proces safe
##
##    should we add an access time column? or even useage statstics?
##    we could, but not much point; there already is a pretty effective memory use limit in play;
##    the disc cleanup up the temporary directory we use
##    """
##
##    def __init__(self, identifier, deferred_timeout = 10, size_limit=100):
##        self.identifier         = identifier                                #name of database file in which to store
##        self.deferred_timeout   = deferred_timeout                          #timeout in seconds to wait for pending action before starting a new one
####        self.locks              = collections.defaultdict(threading.Lock)   #only one thread may modify any given entry at a time
##        self.lock = threading.Lock()  #single lock should do. not much reason to have multiple threads compiling
##        self.size_limit         = size_limit    #size limit in mb. do we really care? we work in a temp directory anyway
##
##        estr  = Pickle.dumps(self.environment())
##        ehash = hashlib.md5(estr).hexdigest()
##        self.filename           = os.path.join(cachepath, identifier+'_'+ehash)
##
##        def create_shelve():
##            """
##            guarantee atomicity of cache creation during multi-processing
##            create database as a tempfile, then rename to actual name when ready
##            """
##
##            tfile = tempfile.NamedTemporaryFile(delete = False)
##            shelve             = Shelve(tfile.name, autocommit=True)
##            #add a meta blob to our shelve, to uniquely describe our environment
##            TABLE='CREATE TABLE IF NOT EXISTS meta (env BLOB)'
##            shelve.conn.execute(TABLE)
##            ADD_ITEM = 'INSERT INTO meta (env) VALUES (?)'
##            shelve.conn.execute(ADD_ITEM, (estr,))
##
##            shelve.close()
##            tfile.close()
##            sleep(0.1)     #wait for file to really close before renaming. surely this can be done better?
##
##            try:
##                os.rename(tfile.name, self.filename)
##                return Shelve(self.filename, autocommit=True)
##            except Exception as e:
##                print e, type(e)
##                #someone beat us to creation
##                os.remove(tfile.name)
##                raise Exception()
##
##        def load_shelve():
##            if os.path.isfile(self.filename):
##                shelve             = Shelve(self.filename, autocommit=True)
##                try:
##                    if not shelve.conn.select_one('SELECT env FROM meta')[0] == estr:
##                        raise Exception()
##                except Exception as e:
##                    print 'no env hit'
##                    print e
##                    shelve.close()
##                    #try and kill the cache with the colliding hash, but unidentical env
##                    #this may fail; perhaps it is in use by another process
##                    #anything we can do about it?
##                    #perhaps we should increment our hash in such a scenario
##                    os.remove(self.filename)
##                    raise Exception()
##                return shelve
##            raise Exception()
##
##        try:
##            self.shelve = load_shelve()
##            return
##        except:
##            pass
##        #cache failed to load; either it didnt exist, or it did exist but had to be removed due to a hash colision
##        try:
##            self.shelve = create_shelve()
##            return
##        except:
##            pass
##        #if creation failed as well, it may be because another process just beat us to it
##        #try loading once more
##        self.shelve = load_shelve()
##
##
##
##
##
##    def __getitem__(self, key):
##        with self.lock:    #want only one thread at a time accessing this; or only one thread per key?
##            while True:
##                try:
##                    value = self.shelve[key]
##                    if isinstance(value, Deferred):
##                        if value.expired(self.deferred_timeout):
##                            raise Exception()
##                        sleep(0.01)
##                    else:
##                        return value
##                except:
##                    self.shelve[key] = Deferred()
##                    #active updating of the deferred object to prove activity?
##                    value = self.cached(key)
##                    self.shelve[key] = value
##                    return value
##
##
##
##
####    def __delitem__(self, key):
####        with self.lock:
####            del self.shelve[key]
##
##
##
##    def cached(self, input):
##        """
##        implements the cached operation; to be invoked upon a cache miss
##        input is a picklable python object
##        the returned output should be a pickalble python object as well
##        """
##        raise NotImplementedError()
##    def environment(self):
##        """
##        returns a pickeable object describing the environment of the cached operation,
##        or a description of the state of your computer which may influence the relation
##        between the input and output of Cache.cached
##        """
##        raise NotImplementedError()



class Cache(object):
    """
    cache object which gracefully handles large key objects

    all relevant state to a compilation process might look like
    (compiler source/version + stdlib, module specifics, source to be compiled, compile time arguments)
    which is mapped to a relatively small output (some object files)
    this entire key object may be used to cache the computation
    however, we do not want to store a copy of the entire stdlib for every function overload.
    even storing the entire per-function source for a combinatorial explosion of overloads may be excessive
    therefore, identical subkeys of this key hierarchy are stored only once in the database

    no particular order is enforced on this key hierarchy, though best practice
    is to put the biggest and/or least variable parts of the key first (say a hash of the stdlib)
    and the most variable parts last (template specialization arguments)

    add dict interface?
    query by partial key and return all underlying results?

    """
    def __init__(self, identifier=None, environment=None, operation=None, validate=False, timeout = 10):
        if identifier: self.identifier = identifier
        if operation: self.operation = operation
        self.environment = environment if environment else self.environment()
        self.validate = validate

        self.filename           = os.path.join(cachepath, self.identifier)
        self.lock = threading.Lock()  #single lock should do. not much reason to have multiple threads compiling
        self.lock_file = lockfile.MkdirLockFile(self.filename, timeout = timeout)

        self.shelve             = Shelve(self.filename, autocommit = True)
        self.shelve.clear()




        #write environment key to database and obtain its unique rowid
        estr, ehash = process_key(self.environment)
        self.shelve.setitem(self.environment, None, estr, ehash)
        self.envrowid = self.shelve.getrowid(self.environment, estr, ehash)





    def __call__(self, *hkey):
        """
        look up a hierachical key object
        fill in the missing parts, and perform the computation at the leaf if so required
        """
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
                            newvalue = self.operation(*hkey)
                            assert(Pickle.dumps(value)==Pickle.dumps(newvalue))
                        return value

                except:
                    #lock for the writing branch. multiprocess does not benefit here, but so be it.
                    #worst case we make multiple insertions into db, but this should do no harm for behavior
                    #can we include a process/NFS safe locking mechanism here?

                    if self.lock.locked():
##                    if False:  #if lock not available, go back to waiting for a deferred to appear
    ##                    pass
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
                            value = self.operation(*hkey)
                            self.shelve.setitem(leafkey, value     , kstr, khash)       #write lock
                            return value





##        with self.lock:    #want only one thread at a time accessing this; or only one thread per key? include NFS locking here?
##            while True:
##                try:
##                    value = self.shelve[key]
##                    if isinstance(value, Deferred):
##                        if value.expired(self.deferred_timeout):
##                            raise Exception()
##                        sleep(0.01)
##                    else:
##                        return value
##                except:
##                    self.shelve[key] = Deferred()
##                    #active updating of the deferred object to prove activity?
##                    value = self.operation(*hkey)
##                    self.shelve[key] = value
##                    return value


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

class CompilationCache(Cache):
    """
    subclass implements the actual cached operation
    """
    identifier = 'theano_compilation'

    def operation(self, source, templates):

        print 'compiling'
        sleep(1)
        return source + str(templates)


    def environment(self):
        version='3.4'
        compiler='llvm'
        return version, compiler


cache = CompilationCache()



class GraphOptimizationCache(Cache):
    identifier = 'theano_graph'

    def cached(self, source):
        n, s = source

        print 'compiling'
        sleep(3)
        q = np.array(list(s*n))
        return np.sort(q).tostring()


    def environment(self):
        import numba
        version = numba.__version__

        import inspect
        files = [numba.Function, numba.Accessor]    #some random files....
        informal_version = [inspect.getsource(file) for file in files]

        return version, informal_version, 999


##cache = CompilationCache('theano')

##cache = GraphOptimizationCache('theano_graph')

##quit()
def worker(arg):
    value = cache(*arg)
    return value


if __name__=='__main__':

    #test compiling the same function many times, or compilaing different functions concurrently
    args = [('The quick brown fox, and so on', 4)]*4
##    args = enumerate( ['The quick brown fox, and so on']*4)

    #run multiple jobs concurrent as either processes or threads
    threading=True
    if threading:
        import multiprocessing.dummy as multiprocessing
    else:
        import multiprocessing

    #sleep(0.1)

    pool = multiprocessing.Pool(4)
    for r in pool.imap(worker,  args):
        print r
##    print cache[(3,'a')]