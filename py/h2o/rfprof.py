# This is a probably temporary file to help Tomas profiling the RF. It uses the debug.json action RF. 

import urllib
import urllib2
import json

DEFAULT_SERVER = "http://localhost:54321/"

KEY = "Key"
KEY2 = "Key2"
ERROR = "Error"
COLUMNS = "columns"
COLUMN_NAME = "name"
CONTENTS = "contents"
EXPR = "Expr"
RESULT_KEY = "ResultKey"
VALUE = "Value"
URL = "Url"
ACTION = "action"

PAGE_IMPORTURL = "ImportUrl.json"
PAGE_REMOVE = "Remove.json"
PAGE_DEBUG = "Debug.json"
PAGE_PARSE = "Parse.json"

class H2OException(Exception):
    
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class H2OConnection:        
    """ This a class that represents the H2O cloud connection. """

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
	
    def importUrl(self,key,url, hex = True):
        """ Imports the given server local file to the H2O and parses it to given key. If hex is False, does not
        parse the file. """
        uploadKey = url if (hex) else key
        res = self._remoteSend(PAGE_IMPORTURL, { KEY : uploadKey,  URL : url })
        if (hex):
	  res = self._remoteSend(PAGE_PARSE, { KEY : res[KEY], KEY2: key})
        return res[KEY]

    def remove(self,key):
        """ Removes the given key. """
        res = self._remoteSend(PAGE_REMOVE, { KEY : key })
        return True
        
    def profileRF(self, Key, ntree=None, depth=None, sample=None, binLimit=None,gini=None,seed=None,parallel=None,modelKey=None,classCol=None, oobee=None, features=None): 
        """ Profiles the RF & CM """
        args = {}
        args["Key"] = Key
        if (ntree != None): args["ntree"] = str(ntree)
        if (depth != None): args["depth"] = str(depth)
        if (sample != None): args["sample"] = str(sample)
        if (binLimit != None): args["binLimit"] = str(binLimit)
        if (gini != None): args["gini"] = str(gini)
        if (seed != None): args["seed"] = str(seed)
        if (parallel != None): args["parallel"] = str(parallel)
        if (modelKey != None): args["modelKey"] = str(modelKey)
        if (classCol != None): args["class"] = str(classCol)
        if (oobee != None): args["OOBEE"] = str(oobee)
        if (features != None): args["features"] = str(features)
        args["action"] = "RF"
        res = self._remoteSend(PAGE_DEBUG, args)
        return (int(res["preprocess"]), int(res["trees"]), int(res["confusion"]))
        
h2o = H2OConnection()
h2o.importUrl("poker","file:///home/peta/poker1000")
print h2o.profileRF("poker", ntree=10)


