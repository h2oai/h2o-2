##
# Create the 'tasks' file
# Each line of tasks contains parameters for building the RUnit
##

import json
from GenUtils import *

def genTasks():
    jsondata = open('./smalldata.json')
    data = json.load(jsondata)
    jsondata.close()
    
    tasks = open('./tasks', 'wb')
    
    for FU in ['[','<','<=','>','>=','==','!=',makeCompound(1),makeCompound(2)]: #,makeCompound(3),makeCompound(4)]:
        achoice = None
        for i in sample(range(len(data['datasets'])), 5): 
        #[choice(range(len(data['datasets']))) for i in range(60)]:
        #for i in range(len(data['datasets'])):
            cnt = achoice
            datajson = data['datasets'][i]
            DATANAME = datajson.keys()[0]
            if FU != '[' and 'number' not in datajson[DATANAME]['ATTRS']['TYPES']: continue
            PATH = '"' + datajson[DATANAME]['PATHS'][0] + '"'
            TESTNAME =  genTestName(FU, DATANAME) 
            DESCRIPTION = '"' + genTestDescription(FU, DATANAME) + '"'
            COLS = ""
            if FU != '[':
                COLS = datajson[DATANAME]['ATTRS']['NAMES']
            if len(FU.split(';')) > 1:
                achoice = 1 
                if cnt is not None: achoice += cnt
            FUPARAMS = generateFUParams(FU, DATANAME, datajson, choiceAs = achoice)
            if FUPARAMS == 'abort': continue
            if FU == '[':
                cols,rows,colPipeRow,loopCols,loopRows,loopColPipeLoopRow = FUPARAMS
                FUPARAMS = ':'.join([TESTNAME, DESCRIPTION, cols,rows,colPipeRow,loopCols,loopRows,loopColPipeLoopRow])
                task = ','.join([TESTNAME,FU,DATANAME,PATH,'filterTask','mungeTask',FUPARAMS])
                tasks.write(task)
                tasks.write('\n')
                continue

            if len(FU.split(';')) > 1:
                valLPipeColL, valRPipeColR, valL2PipeColL2, valR2PipeColR2 = FUPARAMS
                FUPARAMS = ':'.join([TESTNAME,DESCRIPTION,';'.join(COLS),valLPipeColL, valRPipeColR, valL2PipeColL2, valR2PipeColR2])
                task = ','.join([TESTNAME,FU,DATANAME,PATH,'filterTask','mungeTask',FUPARAMS])
                tasks.write(task)
                tasks.write('\n')
                achoice += 1
                continue


            valPipeCol, valPipeCol2 = FUPARAMS
            FUPARAMS = ':'.join([TESTNAME, DESCRIPTION, ';'.join(COLS),valPipeCol, valPipeCol2])
            task = ','.join([TESTNAME,FU,DATANAME,PATH,'filterTask','mungeTask',FUPARAMS])
            tasks.write(task)
            tasks.write('\n')

