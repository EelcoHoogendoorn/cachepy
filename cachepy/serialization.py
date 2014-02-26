
"""
serialization module

includes all requisite machinery to serialize key objects in a determinstic manner
note that for the purposes of caching we do not care about getting the key information back intact;
'destructively' mapping to a characteristic string is fine as far as keys are concerned,
as long as the process is unique and repeatable

specifically, this module aims to serialize dicts and sets in a deterministic manner
also, there is a default deterministic serializer for user defined types
im not sure how robust it is. review needed, plz

note that this code does not handle cyclic references, or reference equality
im not sure its worthwhile to add support for this
it sounds like a lot of work to add, and not much to ask of the end user not to use cyclic references
and not to rely on the difference between reference and value equality in their keys

would we like to use zlib in our encoding?

note that we could subclass Pickle.Pickler, along the lines of joblib
this means we forego cPickle performance
but this may be an acceptable price to pay, for the ability to deal with cyclic references
perhaps we may even want to implement our own serialization inspired by Pickler
memoization is not that hard

"""
##import Pickle

import hashlib
import cPickle as Pickle
import numpy as np
import util

import types
#list of all types without any internal references to other python objects; is this list complete?
flat_types = types.StringTypes +(types.BooleanType, types.BufferType, types.FloatType, types.IntType,  types.LongType, types.NoneType)


def encode(obj):
    return Pickle.dumps(obj, protocol=util.pickle_protocol)    #protocol 2 is the highest protocol which is python2/3 cross compatible. but do we care?

##def decode(obj):
##    return Pickle.loads(str(obj))
##
##def hashing(strobj):
##    return hashlib.sha256(strobj).digest()
##
##def hash_str_to_u64(strobj):
##    """note; we could simply use text hash field rather than int?"""
##    return reduce(np.bitwise_xor, np.frombuffer(hashing(strobj), dtype=np.uint64)) + 1
##
##def process_key(key):
##    keystr = encode(key)
##    keyhash = hash_str_to_u64(keystr)
##    return keystr, keyhash


class DeterministicDict(object):
    """
    wrapper class around dict to ensure it serializes in a determinstic manner
    """
    __slots__ = ['keys', 'values','type']
    def __init__(self, obj):
        keys, values = obj.keys(), obj.values()
        keystrs   = np.array([encode(as_deterministic(key))   for key in keys])
        valuestrs = np.array([encode(as_deterministic(value)) for value in values])
        order     = np.argsort(keystrs)
        self.keys   = keystrs  [order].tolist()     #since we dont care about unpickling our keys anyway...
        self.values = valuestrs[order].tolist()
        self.type   = type(obj)

class DeterministicSet(object):
    """
    deterministic set object
    """
    __slots__ = ['set','type']
    def __init__(self, obj):
        self.set = sorted(encode(as_deterministic(key)) for key in obj)
        self.type = type(obj)

class UserDefinedType(object):
    """dummy type to obtain a list of all members not associated with relevant state"""
import inspect
standard_members = [k for k,v in inspect.getmembers(UserDefinedType())]

class DeterministicUserDefinedType(object):
    """
    a default serializer for user defined types
    almost seems too easy... are there gotchas im missing?
    a key-value mapping and an associated type are basically all python objects are, no?
    """
    __slots__ = ['dict', 'type']
    def __init__(self, obj):
        self.dict = DeterministicDict({k:v for k,v in inspect.getmembers(obj) if not k in standard_members})
        self.type = type(obj)


def as_deterministic(obj):
    """
    recusive mapping of object hierarchies to deterministic equivalents
    note; this is not safe against cyclic dependencies

    inspired by:
    http://stackoverflow.com/questions/985294/is-the-pickling-process-deterministic
    """
    if isinstance(obj, flat_types):     #if the object does not contain internal references, we are happy as-is
        return obj
    if isinstance(obj, types.DictionaryType):
        return DeterministicDict(obj)
    if isinstance(obj, (set, frozenset)):
        return DeterministicSet(obj)
    if isinstance(obj, types.TupleType):
        return tuple(as_deterministic(o) for o in obj)
    elif isinstance(obj, types.ListType):
        return [as_deterministic(o) for o in obj]
    else:
        return DeterministicUserDefinedType(obj)


class DeterministicSerializer(object):
    """
    cut out the pickle middle man? just need our own memo implementation
    note that this serializer does not need to be reversible; only deterministic,
    in the sense that semantically identical objects produce identical output
    """
    def __init__(self, obj):
        self.memo = {}
        import StringIO
        self.buffer = StringIO.StringIO()


if __name__=='__main__':
    class Dummy(object):
        a=3
        def __init__(self):
            self.b=4
    k1, k2 = {1: 0, 9: 0}, {9: 0, 1: 0}

    key = (Dummy(), k1)     #take a rather complex key
    q = as_deterministic(key)
    print encode(q)

