##
# A wrapper script to loop over tasks and produce RUnits
# Each line of tasks is a set of params for building the RUnit
##

import os
from writers import *
from makeTestTasks import genTasks
from GenUtils import setup_random_seed
import GenUtils

def writeFU(FU, data, dataPath, dataTask, taskType, FUParams):
    wt = WriterTask()
    wt.setType(taskType)
    wt.setData(data, dataPath)
    wt.setFUParams(FUParams)
    wt.setFU(FU)
    wt.setDataTask(dataTask)
    return wt.invoke(switch(taskType))

SEED = setup_random_seed()
#s = str(SEED)
#os.system(s +' > seed')

with open("seed", "wb") as f:
    f.write(str(SEED))

genTasks()
os.system('git rev-parse HEAD > githash')

#seed = SEED
#task = None
with open('./tasks', 'rb') as f:
    for line in f:
        with open("task", "wb") as g:
            g.write(line)
        line = line.strip('\n').split(',')
        TESTNAME, FU, data, dataPath, dataTask, taskType, FUParams = line
        test = writeFU(FU,data, dataPath,dataTask,taskType,FUParams)
        with open('./genTests/runit_'+TESTNAME.replace("-","_")+'.R', 'wb') as t:
            t.write(test)

os.system('cd genTests; bash alter.sh')
os.system('cd genTests; bash cleanse.sh')
