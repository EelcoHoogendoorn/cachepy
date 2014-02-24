
"""
shelve2.py, by Eelco Hoogendoorn

this module provides two generalizations over pythons shelve
first of all, it efficiently handles arbitrary (pickable) python objects as keys
secondly, it aims to be entirely process and thread safe.
(note; im not 100% sure this latter goal has entirely been achieved yet)

the design is an sqlite3 database, with a table containing key and value blobs,
which are indexed by a hash of the given key
this allows for efficient mapping of complex python objects to complex python objects

This code is adapted from the sqlitedict code from author below
"""

# Copyright (C) 2011 Radim Rehurek <radimrehurek@seznam.cz>

# Hacked together from:
#  * http://code.activestate.com/recipes/576638-draft-for-an-sqlite3-based-dbm/
#  * http://code.activestate.com/recipes/526618/
#
# Use the code in any way you like (at your own risk), it's public domain.

"""
A lightweight wrapper around Python's sqlite3 database, with a dict-like interface
and multi-thread access support::

>>> mydict = SqliteDict('some.db', autocommit=True) # the mapping will be persisted to file `some.db`
>>> mydict['some_key'] = any_picklable_object
>>> print mydict['some_key']
>>> print len(mydict) # etc... all dict functions work

Pickle is used internally to serialize the values. Keys are strings.

If you don't use autocommit (default is no autocommit for performance), then
don't forget to call `mydict.commit()` when done with a transaction.

"""


import sqlite3
import os
import tempfile
import random
import logging
from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from UserDict import DictMixin
from Queue import Queue
from threading import Thread

import numpy as np
import hashlib


logger = logging.getLogger('sqlitedict')



def open(*args, **kwargs):
    """See documentation of the SqlDict class."""
    return SqliteDict(*args, **kwargs)


def encode(obj):
    """Serialize an object using pickle to a binary format accepted by SQLite."""
    return sqlite3.Binary(dumps(obj, protocol=PICKLE_PROTOCOL))


def decode(obj):
    """Deserialize objects retrieved from SQLite."""
    return loads(str(obj))

def hash_str_to_u64(strobj):
    return reduce(np.bitwise_xor, np.frombuffer(hashlib.sha256(strobj).digest(), dtype=np.uint64)) + 1

def process_key(key):
    keystr = encode(key)
    keyhash = hash_str_to_u64(keystr)
    return keystr, keyhash


class Shelve(object, DictMixin):
    def __init__(self, filename=None, flag='c',
                 autocommit=False, journal_mode="DELETE"):
        """
        Initialize a thread-safe sqlite-backed dictionary. The dictionary will
        be a table `tablename` in database file `filename`. A single file (=database)
        may contain multiple tables.

        If no `filename` is given, a random file in temp will be used (and deleted
        from temp once the dict is closed/deleted).

        If you enable `autocommit`, changes will be committed after each operation
        (more inefficient but safer). Otherwise, changes are committed on `self.commit()`,
        `self.clear()` and `self.close()`.

        Set `journal_mode` to 'OFF' if you're experiencing sqlite I/O problems
        or if you need performance and don't care about crash-consistency.

        The `flag` parameter:
          'c': default mode, open for read/write, creating the db/table if necessary.
          'w': open for r/w, but drop `tablename` contents first (start with empty table)
          'n': create a new database (erasing any existing tables, not just `tablename`!).

        """
        if flag == 'n':
            if os.path.exists(filename):
                os.remove(filename)

        self.filename = filename

##        logger.info("opening Sqlite table %r in %s" % (tablename, filename))
        self.conn = SqliteMultithread(filename, autocommit=autocommit, journal_mode=journal_mode)

##        MAKE_TABLE = 'CREATE TABLE IF NOT EXISTS dict (hash INTEGER PRIMARY KEY, key BLOB, value BLOB)'
        MAKE_TABLE = 'CREATE TABLE IF NOT EXISTS dict (hash INT NOT NULL, key BLOB, value BLOB)'
        self.conn.execute(MAKE_TABLE)
        MAKE_TABLE = 'CREATE INDEX IF NOT EXISTS `id` ON `dict` (`hash` ASC)'
        self.conn.execute(MAKE_TABLE)
        self.conn.commit()

        if flag == 'w':
            self.clear()

    def __str__(self):
#        return "SqliteDict(%i items in %s)" % (len(self), self.conn.filename)
        return "SqliteDict(%s)" % (self.conn.filename)

    def __len__(self):
        # `select count (*)` is super slow in sqlite (does a linear scan!!)
        # As a result, len() is very slow too once the table size grows beyond trivial.
        # We could keep the total count of rows ourselves, by means of triggers,
        # but that seems too complicated and would slow down normal operation
        # (insert/delete etc).
        GET_LEN = 'SELECT COUNT(*) FROM dict'
        rows = self.conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def __bool__(self):
        GET_LEN = 'SELECT MAX(ROWID) FROM dict'
        return self.conn.select_one(GET_LEN) is not None

    def iterkeys(self):
        GET_KEYS = 'SELECT key FROM dict ORDER BY rowid'
        for key in self.conn.select(GET_KEYS):
            yield decode(key[0])

    def itervalues(self):
        GET_VALUES = 'SELECT value FROM dict ORDER BY rowid'
        for value in self.conn.select(GET_VALUES):
            yield decode(value[0])

    def iteritems(self):
        GET_ITEMS = 'SELECT key, value FROM dict ORDER BY rowid'
        for key, value in self.conn.select(GET_ITEMS):
            yield decode(key), decode(value)


    def getrowid(self, key, keystr, keyhash):
        GET_ITEM = 'SELECT rowid, key FROM dict WHERE hash = ?'
        keys = self.conn.select(GET_ITEM, (keyhash,))
        for rowid, storedkey in keys:
            if storedkey == keystr:
                return rowid
        raise KeyError(key)

    def __contains__(self, key):
        try:
            self.getrowid(key, *process_key(key))
            return True
        except:
            return False

    def __getitem__(self, key):
        return self.getitem(key, *process_key(key))
    def getitem(self, key, keystr, keyhash):
        GET_ITEM = 'SELECT key, value FROM dict WHERE hash = ?'
        items = self.conn.select(GET_ITEM, (keyhash,))
        if items is None:
            raise KeyError(key)
        for storedkey, value in items:
            if keystr == storedkey:
                return decode(value)
        raise KeyError(key)

    def __setitem__(self, key, value):
        return self.setitem(key, value, *process_key(key))
    def setitem(self, key, value, keystr, keyhash):
        valuestr = encode(value)
        try:
            rowid = self.getrowid(key, keystr, keyhash)
            ADD_ITEM = 'REPLACE INTO dict (rowid, hash, key, value) VALUES (?,?,?,?)'
            self.conn.execute(ADD_ITEM, (rowid, keyhash, keystr, valuestr))
        except:
            ADD_ITEM = 'INSERT INTO dict (hash, key, value) VALUES (?,?,?)'
            self.conn.execute(ADD_ITEM, (keyhash, keystr, valuestr))

    def __delitem__(self, key):
        self.delitem(key, *process_key(key))
    def delitem(self, key, keystr, keyhash):
        rowid = self.getrowid(key, keystr, keyhash)
        DEL_ITEM = 'DELETE FROM dict WHERE rowid = ?'
        self.conn.execute(DEL_ITEM, (rowid,))


    def update(self, items=(), **kwds):
        def dummy(item):
            key, value = item
            keystr = encode(key)
            keyhash = hash_str_to_u64(keystr)
            valuestr = encode(value)
            return keyhash, keystr, valuestr

        try:
            items = map(dummy, items.iteritems())
        except AttributeError:
            pass

        UPDATE_ITEMS = 'REPLACE INTO dict (hash, key, value) VALUES (?, ?, ?)'
        self.conn.executemany(UPDATE_ITEMS, items)
        if kwds:
            self.update(kwds)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def __iter__(self):
        return self.iterkeys()

    def clear(self):
        CLEAR_ALL = 'DELETE FROM dict;' # avoid VACUUM, as it gives "OperationalError: database schema has changed"
        self.conn.commit()
        self.conn.execute(CLEAR_ALL)
        self.conn.commit()

    def commit(self):
        if self.conn is not None:
            self.conn.commit()
    sync = commit

    def close(self):
        logger.debug("closing %s" % self)
        if self.conn is not None:
            if self.conn.autocommit:
                self.conn.commit()
            self.conn.close()
            self.conn = None

    def terminate(self):
        """Delete the underlying database file. Use with care."""
        self.close()
        logger.info("deleting %s" % self.filename)
        try:
            os.remove(self.filename)
        except IOError, e:
            logger.warning("failed to delete %s: %s" % (self.filename, e))

    def __del__(self):
        # like close(), but assume globals are gone by now (such as the logger)
        try:
            if self.conn is not None:
                if self.conn.autocommit:
                    self.conn.conn.commit()
                self.conn.conn.close()
                self.conn = None
        except:
            pass
#endclass SqliteDict



class SqliteMultithread(Thread):
    """
    Wrap sqlite connection in a way that allows concurrent requests from multiple threads.

    This is done by internally queueing the requests and processing them sequentially
    in a separate thread (in the same order they arrived).

    """
    def __init__(self, filename, autocommit, journal_mode):
        super(SqliteMultithread, self).__init__()
        self.filename = filename
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.reqs = Queue() # use request queue of unlimited size
        self.setDaemon(True) # python2.5-compatible
        self.start()

    def run(self):
        if self.autocommit:
            conn = sqlite3.connect(self.filename, isolation_level=None, check_same_thread=False)
        else:
            conn = sqlite3.connect(self.filename, check_same_thread=False)
        conn.execute('PRAGMA journal_mode = %s' % self.journal_mode)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        while True:
            req, arg, res = self.reqs.get()
            if req == '--close--':
                break
            elif req == '--commit--':
                conn.commit()
            else:
                cursor.execute(req, arg)
                if res:
                    for rec in cursor:
                        res.put(rec)
                    res.put('--no more--')
                if self.autocommit:
                    conn.commit()
        conn.close()

    def execute(self, req, arg=None, res=None):
        """
        `execute` calls are non-blocking: just queue up the request and return immediately.

        """
        self.reqs.put((req, arg or tuple(), res))

    def executemany(self, req, items):
        for item in items:
            self.execute(req, item)

    def select(self, req, arg=None):
        """
        Unlike sqlite's native select, this select doesn't handle iteration efficiently.

        The result of `select` starts filling up with values as soon as the
        request is dequeued, and although you can iterate over the result normally
        (`for res in self.select(): ...`), the entire result will be in memory.

        """
        res = Queue() # results of the select will appear as items in this queue
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            if rec == '--no more--':
                break
            yield rec

    def select_one(self, req, arg=None):
        """Return only the first row of the SELECT, or None if there are no matching rows."""
        try:
            return iter(self.select(req, arg)).next()
        except StopIteration:
            return None

    def commit(self):
        self.execute('--commit--')

    def close(self):
        self.execute('--close--')
#endclass SqliteMultithread


##quit()

# running sqlitedict.py as script will perform a simple unit test
if __name__ in '__main___':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(module)s:%(lineno)d : %(funcName)s(%(threadName)s) : %(message)s')
    logging.root.setLevel(level=logging.INFO)
    for d in Shelve(r'c:\docs\testdb'), Shelve(r'c:\docs\testdb2', flag='n'):
        assert list(d) == []
        assert len(d) == 0
        assert not d
        d['abc'] = 'rsvp' * 100
        assert d['abc'] == 'rsvp' * 100
        assert len(d) == 1
        d['abc'] = 'lmno'
        assert d['abc'] == 'lmno'
        assert len(d) == 1
        del d['abc']
        assert not d
        assert len(d) == 0
        d['abc'] = 'lmno'
        d['xyz'] = 'pdq'
        assert len(d) == 2
        assert list(d.iteritems()) == [('abc', 'lmno'), ('xyz', 'pdq')]
        assert d.items() == [('abc', 'lmno'), ('xyz', 'pdq')]
        assert d.values() == ['lmno', 'pdq']
        assert d.keys() == ['abc', 'xyz']
        assert list(d) == ['abc', 'xyz']
        d.update(p='x', q='y', r='z')
        assert len(d) == 5
        assert d.items() == [('abc', 'lmno'), ('xyz', 'pdq'), ('q', 'y'), ('p', 'x'), ('r', 'z')]
        del d['abc']
        try:
            error = d['abc']
        except KeyError:
            pass
        else:
            assert False
        try:
            del d['abc']
        except KeyError:
            pass
        else:
            assert False
        assert list(d) == ['xyz', 'q', 'p', 'r']
        assert d
        d.clear()
        assert not d
        assert list(d) == []
        d.update(p='x', q='y', r='z')
        assert list(d) == ['q', 'p', 'r']
        d.clear()
        assert not d
        d.close()
    print 'all tests passed :-)'
