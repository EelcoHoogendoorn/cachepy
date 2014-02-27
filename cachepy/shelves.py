

"""
module containing code attempting to generalize the key-value store used,
and the behaviors defined on top of them

a local and a distributable pycc cache have quite different needs, for instance
it would be nice if we can make all relevant features and their permutations optional though

the basic design is to have several orthodox key-value mappings
(dict, shelve, sqldict, and so on),
which vary in their characteristics


"""

class Dict(dict):
    def __init__(
            self,
            determinstic=True,  #type of serialization. deterministic is slower, but necessary for pycc type caching
            exact=True,         #store full keys, or only their hashes
            readonly=False
            ):
        """
        light dict wrapper
        """
        def err(self, args):
            raise Exception('This is a readonly mapping')
        if readonly: self.__setitem__ = err

    def __setitem__(self, key, value):
        key = key   #serialize keys
        self[key] = value


class Wrapper(object):
    """
    class which wraps a key-value mapping with several (optional) features:
        locking
        key serialization
        hierarchical keying scheme
    """

    def __init__(self, shelve, locking, deterministic, hierarchical, zip = True):
        self.shelve         = shelve
        self.locking        = locking
        self.deterministic  = deterministic
        self.hierarchical   = hierarchical
        aelf.zip            = True      #zip the key/value pickles. to be preferred for long term storage, but not for memoization
        if deterministic:
            def encode(key):
                return key
            def encode(key):
                return key

        self.encode = encode


def select_mapping(
        local=True,         #if true, sqlite, else remote server
        ondisk=True,        #persistent disk based or per session in mem cache
        locking='file',     #locking scheme used to regulate access for local ondisk backends

        hierarchical=False, #hierarchical key storage scheme
        determinstic=True,  #type of serialization. deterministic is slower, but necessary for pycc type caching
        exact=True,         #store full keys, or only their hashes
        readonly=False,     #whether writing keys is allowed. this has implications for locking
        ):
    """
    select a key-value mapping with the appropriate chacteristics
    shelve2 is the most general implementation thus far,
    but there are many functionality/performance tradeoffs to be made
    """
    if readonly: locking = None

    if local and ondisk and determinstic and exact:
        import shelve2
        return shelve2.Shelve

    if readonly and ondisk and local and not exact and determinstic:
        import ReadOnlyShelve
        return ReadOnlyShelve.Shelve

    if not locking and local and ondisk:
        #create shelve wrapper with different accessors
        import shelve
        return shelve.Shelf

    if local and not ondisk:
        return lambda args: Dict(determinstic, exact, readonly)



    raise NotImplementedError('No key value store matching these criteria has been found')