

"""
module containing code to generalize the key-value store used
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


def select_mapping(
        local=True,         #if true, sqlite, else remote server
        ondisk=True,        #persistent disk based or per session in mem cache
        locking='file',     #locking scheme used to regulate access for local ondisk backends

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