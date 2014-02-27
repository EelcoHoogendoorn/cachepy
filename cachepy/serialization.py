
"""
deterministic pickling of python objects
the problem which this module solves is that
dumps({1: 0, 9: 0}) != dumps({9: 0, 1: 0})
using as_deterministic instead of dumps, we are good

code adapted from joblib, as indicated below

one weakness i have found so far is that numpy array view relationships are flattend
in the pickling process. not likely to be an immediate issue, but good to be aware of
i couldnt guarantee that there arnt any other gotchas like that


"""


# Author: Gael Varoquaux <gael dot varoquaux at normalesup dot org>
# Copyright (c) 2009 Gael Varoquaux
# License: BSD Style, 3 clauses.

import warnings
import pickle
import hashlib
import sys
import types
import struct
import util

import io

if sys.version_info[0] < 3:
    Pickler = pickle.Pickler
else:
    Pickler = pickle._Pickler


class _ConsistentSet(object):
    """ Class used to ensure the hash of Sets is preserved
        whatever the order of its items.
    """
    def __init__(self, set_sequence):
        self._sequence = sorted(set_sequence)


class _MyHash(object):
    """ Class used to hash objects that won't normally pickle """

    def __init__(self, *args):
        self.args = args


class DeterministicPickler(Pickler):
    """
    A subclass of pickler, to produce a deterministic result
    unpickable things like functions are irreversably serialized
    """

    def __init__(self):
        self.stream = io.BytesIO()
        Pickler.__init__(self, self.stream, protocol=util.pickle_protocol)

    def dumps(self, obj):
        try:
            self.dump(obj)
        except pickle.PicklingError as e:
            warnings.warn('PicklingError while pickling %r: %r' % (obj, e))
        return str(self.stream.getvalue())

    def save(self, obj):
        if isinstance(obj, (types.MethodType, type({}.pop))):
            # the Pickler cannot pickle instance methods; here we decompose
            # them into components that make them uniquely identifiable
            if hasattr(obj, '__func__'):
                func_name = obj.__func__.__name__
            else:
                func_name = obj.__name__
            inst = obj.__self__
            if type(inst) == type(pickle):
                obj = _MyHash(func_name, inst.__name__)
            elif inst is None:
                # type(None) or type(module) do not pickle
                obj = _MyHash(func_name, inst)
            else:
                cls = obj.__self__.__class__
                obj = _MyHash(func_name, inst, cls)
        Pickler.save(self, obj)

    # The dispatch table of the pickler is not accessible in Python
    # 3, as these lines are only bugware for IPython, we skip them.
    def save_global(self, obj, name=None, pack=struct.pack):
        # We have to override this method in order to deal with objects
        # defined interactively in IPython that are not injected in
        # __main__
        kwargs = dict(name=name, pack=pack)
        if sys.version_info >= (3, 4):
            del kwargs['pack']
        try:
            Pickler.save_global(self, obj, **kwargs)
        except pickle.PicklingError:
            Pickler.save_global(self, obj, **kwargs)
            module = getattr(obj, "__module__", None)
            if module == '__main__':
                my_name = name
                if my_name is None:
                    my_name = obj.__name__
                mod = sys.modules[module]
                if not hasattr(mod, my_name):
                    # IPython doesn't inject the variables define
                    # interactively in __main__
                    setattr(mod, my_name, obj)

    dispatch = Pickler.dispatch.copy()
    # builtin
    dispatch[type(len)] = save_global
    # type
    dispatch[type(object)] = save_global
    # classobj
    dispatch[type(Pickler)] = save_global
    # function
    dispatch[type(pickle.dump)] = save_global

    def _batch_setitems(self, items):
        # forces order of keys in dict to ensure consistent hash
        Pickler._batch_setitems(self, iter(sorted(items)))

    def save_set(self, set_items):
        # forces order of items in Set to ensure consistent hash
        Pickler.save(self, _ConsistentSet(set_items))

    dispatch[type(set())] = save_set



import numpy as np
class ndarray_own(object):
    def __init__(self, arr):
        self.buffer     = np.getbuffer(arr)
        self.dtype      = arr.dtype
        self.shape      = arr.shape
        self.strides    = arr.strides

class ndarray_view(object):
    def __init__(self, arr):
        self.base       = arr.base
        self.offset     = self.base.ctypes.data - arr.ctypes.data   #so we have a view; but where is it?
        self.dtype      = arr.dtype
        self.shape      = arr.shape
        self.strides    = arr.strides


class NumpyDeterministicPickler(DeterministicPickler):
    """
    Special case for numpy.
    in general, external C objects may include internal state which does not serialize in a way we want it to
    ndarray memory aliasing is one of those things
    """

    def __init__(self, coerce_mmap=False):
        """
            Parameters
            ----------
            hash_name: string
                The hash algorithm to be used
            coerce_mmap: boolean
                Make no difference between np.memmap and np.ndarray
                objects.
        """
        self.coerce_mmap = coerce_mmap
        DeterministicPickler.__init__(self)
        # delayed import of numpy, to avoid tight coupling
        import numpy as np
        self.np = np
        if hasattr(np, 'getbuffer'):
            self._getbuffer = np.getbuffer
        else:
            self._getbuffer = memoryview

    def save(self, obj):
        """
        remap a numpy array to a representation which conserves
        all semantically relevant information concerning memory aliasing
        note that this mapping is 'destructive'; we will not get our original numpy arrays
        back after unpickling. this is only meant to be used to obtain correct keying behavior
        better to use a dummy class, to avoid key collisions
        """
        if isinstance(obj, self.np.ndarray):
            if obj.flags.owndata:
                obj = ndarray_own(obj)
            else:
                obj = ndarray_view(obj)
        DeterministicPickler.save(self, obj)




##        if isinstance(obj, self.np.ndarray) and not obj.dtype.hasobject:
##            # Compute a hash of the object:
##            try:
##                self._hash.update(self._getbuffer(obj))
##            except (TypeError, BufferError, ValueError):
##                # Cater for non-single-segment arrays: this creates a
##                # copy, and thus aleviates this issue.
##                # XXX: There might be a more efficient way of doing this
##                # Python 3.2's memoryview raise a ValueError instead of a
##                # TypeError or a BufferError
##                self._hash.update(self._getbuffer(obj.flatten()))
##
##            # We store the class, to be able to distinguish between
##            # Objects with the same binary content, but different
##            # classes.
##            if self.coerce_mmap and isinstance(obj, self.np.memmap):
##                # We don't make the difference between memmap and
##                # normal ndarrays, to be able to reload previously
##                # computed results with memmap.
##                klass = self.np.ndarray
##            else:
##                klass = obj.__class__
##            # We also return the dtype and the shape, to distinguish
##            # different views on the same data with different dtypes.
##
##            # The object will be pickled by the pickler hashed at the end.
##            obj = (klass, ('HASHED', obj.dtype, obj.shape, obj.strides))
##        DeterministicPickler.save(self, obj)

def as_deterministic(obj):
##    return pickle.dumps(obj)
    return NumpyDeterministicPickler().dumps(obj)




if __name__=='__main__':

    k1, k2 = {1: 0, 9: 0}, {9: 0, 1: 0}

    print len(as_deterministic(k1))
    print pickle.loads(as_deterministic(k2))