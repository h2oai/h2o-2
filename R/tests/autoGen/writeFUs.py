##
# A wrapper script to loop over tasks and produce RUnits
# Each line of tasks is a set of params for building the RUnit
##

import os
from writers import *
from makeTestTasks import genTasks
from GenUtils import setup_random_seed

def writeFU(FU, data, dataPath, dataTask, taskType, FUParams):
    wt = WriterTask()
    wt.setType(taskType)
    wt.setData(data, dataPath)
    wt.setFUParams(FUParams)
    wt.setFU(FU)
    wt.setDataTask(dataTask)
    return wt.invoke(switch(taskType))

SEED = setup_random_seed()
with open('./SEED', 'wb') as f:
    f.write(str(SEED))

genTasks()

with open('./tasks', 'rb') as f:
    for line in f:
        line = line.strip('\n').split(',')
        TESTNAME, FU, data, dataPath, dataTask, taskType, FUParams = line
        test = writeFU(FU,data, dataPath,dataTask,taskType,FUParams)
        with open('./genTests/'+TESTNAME+'.R', 'wb') as t:
            t.write(test)

os.system('cd genTests; bash alter.sh')
os.system('cd genTests; bash cleanse.sh')
