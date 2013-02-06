import urllib
import urllib2
import json

from key import Inspect, Key

from .definitions import *

# Python <-> H2O interop layer
# 
# Similar to R's version. It may be slightly different due to language differences. Does not yet support any of the
# Python scientific frameworks because I am not that knowledgeable with them. Imports and exports are only done
# to/from python lists and dictionaries.
#
# I plan to keep it very minimalistic, so no functionality provided from 3rd party libraries.
# 
# Can do put, get, import, exec in the way similar to R's interop.  

# ---------------------------------------------------------------------------------------------------------------------

class H2OException(Exception):
    
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
# ---------------------------------------------------------------------------------------------------------------------

class Cloud:
    """ This a class that represents the H2O cloud. """

    def __init__(self, server=DEFAULT_SERVER, verbose=True):
        """ Creates the h2o object connecting to given server. """
        self._server = server
        if (self._server[-1] != '/'):
            self._server += '/'
        self._verbose = verbose

    def _print(self,what, prefix=""):
        """ Debug print, if verbose mode is on. """
        if (not self._verbose):
            return
        print prefix+what

    def _remoteSend(self, action, args={}):
        """ Sends the given request to the server and returns the JSON object result. """
        data = urllib.urlencode(args)
        c = urllib2.urlopen(self._server+action,data)
        res = json.loads(c.read())
        self._checkError(res)
        return res

    def _checkError(self,res):
        if (ERROR in res):
          self._print(res[ERROR])
          raise H2OException(res[ERROR])

    def _keyToDict(self,res):
        """ Converts the given response key to a python dict object """
        result = {}
        if (len (res[COLUMNS]) == 1):
            return res[COLUMNS][0][CONTENTS]
        for col in res[COLUMNS]:
            result[col[COLUMN_NAME]] = col[CONTENTS]  #[float(x) for x in col[self.CONTENTS].split(" ")]
        return result

    def __call__(self,expr):
        """ expression to evaluate on the server and return. """
        res = self._execExpr(expr)
        return self.get(res[RESULT_KEY])

    def _execExpr(self,expr):
        return  self._remoteSend(PAGE_EXEC, { EXPR : expr })

    def get(self,key):
        """ Gets the given key. """
        try:

            res = self._remoteSend(PAGE_GET, { KEY : key })
            return Key(self,key,self._keyToDict(res))
        except H2OException:
            return None

    def put(self,key,what):
        """ Puts the given list to the H2O as a key with given name. """
        if (type(what) == list):
            res = self._remoteSend(PAGE_PUT, { KEY : key, VALUE : " ".join([str(x) for x in what]) })
            return self.get(key)
        else:
            raise NotImplementedError("At the moment only lists can be stored to H2O clouds.")
        

    def importFile(self,key,url, hex = True):
        """ Imports the given server local file to the H2O and parses it to given key. If hex is False, does not
        parse the file. """
        uploadKey = url if (hex) else key
        res = self._remoteSend(PAGE_IMPORTURL, { KEY : uploadKey,  URL : file })
        return Key(self,res[KEY])

    def inspect(self,key):
        """ Inspects the given key """
        return Key(self,key)

    def remove(self,key):
        """ Removes the given key. """
        res = self._remoteSend(PAGE_REMOVE, { KEY : key })
        return True

    def __getitem__(self,key):
        """ Returns a given key, or None if the key does not exist """
        result = self.get(key)
        if (result == None):
            raise KeyError("Key %s not found in %s" % (key, self._h2o))
        return result

    def __setitem__(self,key,value):
        """ Shorthand for put API call. """
        self.put(key,value)


    def __delitem__(self,key):
        pass

    def __repr__(self):
        return "H2O cloud at %s" % self._server
