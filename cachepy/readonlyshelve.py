"""
read only pycc type cache
it is built once from a list of key value pairs, and cannot be appended to later
keys are not stored in the database; only their hashes
hash colisions are checked for at creation-time

we could store keys with offsets into the binary file to load values lazily,
but given that we typically use pretty much all functions in a pycc shelve
at each program run, we might as well just pickle a whole dict
only pre-pickle the values to avoid unnecessary memory fragmentation

note that this shelve is not intended to be used directly, but rather
intended to be managed from within a Cache object, which defines its interaction with the code
"""

import cPickle as Pickle
import numpy as np
import hashlib
from serialization import as_deterministic
import gzip     #global zip of our cache may be worthwhile
import util


def pickling(obj):
    return Pickle.dumps(obj, protocol=util.pickle_protocol)
def hashing(obj):
    return hashlib.sha256(pickling(as_deterministic(obj))).digest()

class ReadOnlyShelve(object):
    """
    read only shelve object
    note that this isnt a lazy store; a dict is saved and loaded as one piece
    this is appropriate if a large fraction of the information is expected to be used at each run

    """
    def __init__(self, filename):
        self.filename = filename

        self.shelve = Pickle.load(gzip.open(self.filename,'rb'))

    def __getitem__(self, key):
        return Pickle.loads(self.shelve[hashing(key)])


    @staticmethod
    def build(filename, items):
        """
        build pycc cache from a key-values pair iterable and write it to a filename
        """
        keys, values = zip(*items)

        hashes = np.array( [hashing (key)   for key   in keys])
        values = np.array( [pickling(value) for value in values])
        assert np.unique(hashes).size == hashes.size, \
            'Holy shit, 256 bit hash collision! Make some superficial changes to your code to make this go away!'
        Pickle.dump(
            dict(zip(hashes, values)),
            gzip.open(filename, 'wb'),
            protocol=util.pickle_protocol)



if __name__=='__main__':
    #create some random junk data, including a nontrivial key
    k1, k2 = {1: 0, 9: 0}, {9: 0, 1: 0}
    items = [('a', 4), ('b', 30), ('eelco',3)]
    items.append((k1, 'value'))

    import tempfile
    filename = tempfile.mktemp()
    ReadOnlyShelve.build(filename, items)
    rs = ReadOnlyShelve(filename)

    #lets see if this works:
    print rs['a']
    k1, k2 = {1: 0, 9: 0}, {9: 0, 1: 0}

    print rs[k1]
    print rs[k2]

