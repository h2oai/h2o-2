from definitions import *

# ---------------------------------------------------------------------------------------------------------------------

class ColumnInfo:
    """ Infor for a single column in the H2O data frame. """

    def __init__(self, res):
        """ Creates the ColumnInfo object from the resource dictionary (i.e. one returned by the JSON api) """
        self._scale = res["scale"]
        self._offset = res["off"]
        self.name = res["name"]
        self.min = res["min"]
        self.max = res["max"]
        self.badAt = res["badat"]
        self._base = res["base"]
        self.var = res["var"]
        self.mean = res["mean"]
        self.type = res["type"]
        self._size = res["size"]

    def __repr__(self,prefix=""):
        """ Converts the column info to string containg column's name and the reported statistics. """
        return prefix + "%s: min %s, max %s, mean %s, var %s, badAt %s, type %s" % (self.name, self.min, self.max, self.mean, self.var, self.badAt, self.type)

# ---------------------------------------------------------------------------------------------------------------------

class Columns:
    """ Container for holding the information about the columns in the data frame. """

    def __init__(self, res):
        """ Creates the columns container from the resource dictionary. """
        self._columns = []
        for col in res:
            cinfo = ColumnInfo(col)
            setattr(self,cinfo.name,cinfo)
            self._columns.append(cinfo)
        
    def  __getitem__(self,key):
        if (type(key) == int):
            return self._columns[key]
        else:
            return getattr(self,key)

    def __len__(self):
        return len(self._columns)

    def __repr__(self,prefix=""):
        return "\n".join([ x.__repr__(prefix) for x in self._columns])

# ---------------------------------------------------------------------------------------------------------------------

class Inspect:
    """ Result of the H2O API Inspect all - information about the key's value and columns without the actual data. """

    def __init__(self,h2o,name):
        self._h2o = h2o
        self.name = name
        self.refresh()

    def refresh(self):
        res = self._h2o._remoteSend(PAGE_INSPECT, { KEY : self.name })
        self.rows = res["rows"]
        self._rowSize = res["rowsize"]
        self.cols = res["cols"]
        self._priorKey = res["priorKey"]
        self.key = res["key"]
        self.type = res["type"]
        self._size = res["size"]
        self.columns = Columns(res["columns"])

    def __repr__(self,prefix=""):
        result = "Key %s: %s rows x %s cols (type %s):" % (self.name, self.rows, self.cols, self.type)
        for col in self.columns:
            result += "\n" + col.__repr__("  "+prefix)
        return result

# ---------------------------------------------------------------------------------------------------------------------

class Key(Inspect):
    """ This is a class for an H2O key and value. It can be inspected, value obtained, deleted and obtained from the
    H2O cloud. The key also supports basic arithmetic operations with other H2O keys and Python scalars and lists. If 
    I decide it makes sense. 

    At the moment it is just a demonstrator. 
    """

    def __init__(self,h2o,name, value=None):
        """ Creates the key and optionally fills it with value. The inspect request is always sent to the cloud so that
        the basic information about the key can be obtained. """
        Inspect.__init__(self, h2o, name)
        self._name = name
        self._value = value

    def get(self):
        """ Returns the value of the key. If the value is cached, returns the cached value, otherwise connects to the
        server to get the value. You may always call the invalidate() method to 
        """
        if (self._value == None):
            try:
              res = self._h2o._remoteSend(PAGE_GET, { KEY : self._name })
              self._value = self._h2o._keyToDict(res)
            except H2OException:
              pass
        return self._value

    def invalidate(self):
        self._value = None

    def inspect(self):
        self.refresh()

    def remove(self):
        return self._h2o.remove(self._name)

    def __repr__(self,prefix=""):
        return Inspect.__repr__(self,prefix) + "\n"+prefix + ("(loaded)" if self._value != None else "")

    def __str__(self):
        return self._name

    def __iadd__(self, other):
        if (type(other) == int):
            self._h2o._execExpr("%s = %s + %s" % (self._name, self._name, other))
            self._value = None
            self.inspect()
            return self
        else:
            print("error")
