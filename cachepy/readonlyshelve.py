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


def pickling(obj):
    return Pickle.dumps(obj, protocol=-1)
def hashing(obj):
    return hashlib.sha256(pickling(obj)).digest()

class ReadOnlyShelve(object):
    def __init__(self, filename):
        self.filename = filename

        self.shelve = Pickle.load(open(self.filename,'rb'))
##        self.handle = open(filename, 'rb')
##        self.keys = None
##        self.value = None

    def __getitem__(self, key):
        return Pickle.loads(self.shelve[hashing(key)])
##        i = np.searchsorted(self.keys, hash)
##        value = self.values[i]
##        return Pickle.loads(value)


    @staticmethod
    def build(shelve, filename):
        """
        build pycc cache from a dict-like and write it to a filename
        """

        hashes = np.array( [hashing (key)   for key   in shelve.keys()])
        values = np.array( [pickling(value) for value in shelve.values()])
        assert np.unique(hashes).size == hashes.size, 'Holy shit, 256 bit hash collision! Make some superficial changes to your code to make this go away!'
        Pickle.dump(dict(zip(hashes, values)), open(filename, 'wb'), protocol=-1)
##        I = np.argsort(hashes)
##
##        values = np.array([pickling(value) for value in values])
##
##        keys   = keys[I]
##        values = values[I]
##
##        handle = open(filename, 'wb')
##        Pickle.dump((keys, values), handle, protocol=-1)



if __name__=='__main__':
    import tempfile
    d = dict(a=4, b=30, eelco=3)
    filename = tempfile.mktemp()
    ReadOnlyShelve.build(d, filename)
    rs = ReadOnlyShelve(filename)
    print rs['a']
