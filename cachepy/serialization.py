
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
"""


import hashlib
import cPickle as Pickle
import numpy as np


import types
#list of all types without any internal references; is this list complete?
flat_types = types.StringTypes +(types.BooleanType, types.BufferType, types.FloatType, types.IntType,  types.LongType, types.NoneType)


def encode(obj):
    return Pickle.dumps(obj, protocol=-1)

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
    __slots__ = ['keys', 'values']
    def __init__(self, dict):
        keys, values = dict.keys(), dict.values()
        keystr = [deterministic_serialization(key) for key in keys]
        order = np.argsort(keystr)
        self.keys   = np.array(keys)[order].tolist()
        self.values = np.array(values)[order].tolist()

class DeterministicSet(object):
    """
    deterministic set object
    """
    __slots__ = ['set']
    def __init__(self, set):
        keystr = [deterministic_serialization(key) for key in set]
        order = np.argsort(keystr)
        self.set   = np.array(set)[order].tolist()


class DeterministicUserDefinedType(object):
    """
    a default serializer for user defined types
    not sure how general this is...
    data attributes starting with __ will be missed, for instance
    """
    __slots__ = ['dict', 'type']
    def __init__(self, obj):
        import inspect
        self.dict = DeterministicDict(dict([(k,v) for k,v in inspect.getmembers(obj) if not k.startswith('__')]))
        self.type = type(obj)


def deterministic_serialization(obj):
    """
    recusive deterministic serialization
    note; this is not safe against cyclic dependencies

    inspired by:
    http://stackoverflow.com/questions/985294/is-the-pickling-process-deterministic
    """
    if isinstance(obj, flat_types):     #if the object does not contain internal references, we are happy
        return obj
    if isinstance(obj, types.DictionaryType):
        return DeterministicDict(obj)
    if isinstance(obj, (set, frozenset)):
        return DeterministicSet(obj)
    if isinstance(obj, types.TupleType):
        return tuple(deterministic_serialization(o) for o in obj)
    elif isinstance(obj, types.ListType):
        return [deterministic_serialization(o) for o in obj]
    else:
        return DeterministicUserDefinedType(obj)



if __name__=='__main__':
    class Dummy(object):
        a=3
        def __init__(self):
            self.b=4
    key = (Dummy(), {3:4, 5:6})
    q = deterministic_serialization()
    print encode(q)

