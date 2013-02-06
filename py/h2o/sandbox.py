import sys
import os
sys.path.append(os.getcwd())

import h2o

h2o = h2o.Cloud()

h2o["haha"] = [1,2,3,4,5,6,7,8,9,10]

y = h2o["haha"]

y += 5

y.invalidate()
print y.__repr__()

print y.get()

print y.__repr__()


#print y
#print y.get()

#y += 5

#print y.__str__("")
        
#x = h2o.inspect("haha")
#print x.key
#print x.name
#print x.columns
#print x.rows
#print x.columns[0].min
#print x.columns[0].max
#print x.columns[0].mean


#print h2o.get("haha")

#print h2o.inspect("haha")

