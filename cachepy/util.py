"""
cachepy utility functions

we may commonly want to read a folder hierarchy of files from disk
"""

import cPickle as Pickle

pickle_protocol = -1        #set to 2 for python 2/3 compatibility

def read_directory(root, whitelist, blacklist):
    import os
    return

