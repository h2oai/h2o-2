##
# Helper functions used to auto-generate R unit tests
##

from uuid import uuid4
from random import randrange, uniform, random, choice, sample, randint, seed
from math import floor, ceil
import sys

count = 0
def setup_random_seed(random_seed=None):
    if random_seed is not None:
        SEED = random_seed
    else:
        SEED = randint(0, sys.maxint)
    seed(SEED)
    print "\nUsing random seed:", SEED 
    return SEED

def quoteIt(it):
    try:
        x = float(it)
        if abs(x - floor(x)) in [0.0, 1.0]:
            return int(x)
        return float(it)
    except:
        return '"' + it + '"'

#takes a ';' separated string and creates an R style vector c(...)
def makeVec(item):
    it = item.split(';')
    it = [i.replace('"','') for i in it]
    it = [quoteIt(i) for i in it]
    it = makeChar(it)
    it = list(set(it))
    it = filter(lambda a: a != 0,it)
    it = filter(lambda a: a != '0',it)
    if it == []:
        it = ['1']
    if it == ['']: 
        it = ['1']
    if it == ['""']: 
        it = ['1']
    res = 'c(' + ','.join(it) + ')' if item != '0' else '0' 
    return res

def makeElm(e):
    return 'c(' + e + ')'

def makeCharVec(item):
    item = item.split(';')
    res = 'c('
    for i,it in enumerate(item):
        res += "\""+it+"\"," if i != (len(item)-1) else "\""+it+"\")"
    return res

def escape(E):
    return E.replace('"', '\\"')

#takes a pair of val-col pair (val, col) and an operator FU
#FU is in ['<', '!=', '>=', '<=', '>', '==']
#aux is possibly '!' if it's passed in
def makeUnit(vc, FU):
    #{0} is the operator FU
    #{1} is vc[1], the column
    #{2} is vc[0], the value
    return '( hex[,{1}] {0} {2} )'.format(FU, makeVec(vc[1]), vc[0])

def makeExpression(O, vcPairs):
    O = filter(None, O)
    endParen = False
    EXPRESSION = ""
    if O[0] == '!':
        EXPRESSION += "!( "
        endParen = True
        O.pop(0)
    for vcPair in vcPairs:
        #vcpair[0] is the LHS (val, col) pair i
        #vcpair[1] is the RHS (val, col) pair i
        #{0}  is the complex expression
        #{1}  is DATANAME
        if len(O) == 1:
            EXPRESSION += '( ' + makeUnit(vcPair[0],O.pop(0)) + ") "
            break
        if O == []:
            break
        if O[0] == '!':
            O.pop(0)
            if len(O) == 1:
                EXPRESSION += '!( ' + makeUnit(vcPair[0],O.pop(0)) + ')' + ' '
                break
            else:
                EXPRESSION += '!( ' + makeUnit(vcPair[0],O.pop(0)) + ' ' + O.pop(0) + ' ' + makeUnit(vcPair[1], O.pop(0)) + ')' + ' '
            continue
        if O[0] == '(':
            O.pop(0)
            if len(O) == 1:
                EXPRESSION += '( ' + makeUnit(vcPair[0],O.pop(0)) + ' )' + ' '
                break
            else:
                EXPRESSION += '( ' + makeUnit(vcPair[0],O.pop(0)) + ' ' + O.pop(0) + ' ' + makeUnit(vcPair[1], O.pop(0)) + ')' 
            continue
        if O[0] == '&':
            O.pop(0)
            EXPRESSION += ' ' + '&' + ' '
        if O[0] == '|':
            O.pop(0)
            EXPRESSION += ' ' + '|' + ' '
    
        if len(O) == 1:
            EXPRESSION += '( ' + makeUnit(vcPair[0],O.pop(0)) + ') '
            break
        EXPRESSION += makeUnit(vcPair[0], O.pop(0)) + ' ' + O.pop(0) + ' ' + makeUnit(vcPair[1], O.pop(0))

    if endParen: EXPRESSION += ')' 
    return EXPRESSION

def makeCompound(choiceAs):
    A = ['<','<=','>','>=','==','!=']
    B = ['&','|']
    C = ['&','|','&;!' '|;!']
    start = [choice(['','!'])]
    while choiceAs > 0:
        if choiceAs == 1:
            start.append(choice(A))
            return ';'.join(start)
        start.append(choice(A))
        start.append(choice(B))
        start.append(choice(A))
        start.append(choice(C)) 
        choiceAs -= 2
        if choiceAs <= 0:
            start.append(choice(A))
            return ';'.join(start)

def getCol(colValDict):
    while(1):
        col = choice(colValDict.keys())
        if colValDict[col][0] != 'NA':
            return col

def makeChar(seq):
    for x in seq:
        yield str(x)

def convertSeq(seq):
    for x in seq:
        try:
            yield float(x)
        except:
            yield str(x)

def genTestName(FU, dataname):
    global count
    count += 1
    res = dataname + '_' + str(count) #str(uuid4()).replace('-','_')
    if FU == '[': 
        return 'sliceTest_' + res
    if len(FU.split(';')) > 1:
        return 'complexFilterTest_' + res
    return 'simpleFilterTest_' + res

def genTestDescription(FU, dataname):
    res = " on data {0}".format(dataname) 
    if FU == '[': 
        return 'sliceTest_' + res 
    if len(FU.split(';')) > 1:
        return 'compoundFilterTest_' + res 
    return 'simpleFilterTest_' + res 

def generateFUParams(FU, dataname, datajson, choiceAs = None):
    numCols = int(datajson[dataname]['ATTRS']['NUMCOLS'])
    numRows = int(datajson[dataname]['ATTRS']['NUMROWS'])
    if numRows == 0 or numCols == 0:
        return 'abort'
    if numCols >= 50:
        numCols = 15
    if numRows >= 10000:
        numRows = 500
    names = datajson[dataname]['ATTRS']['NAMES']
    if names[0] == 0:
        names = [i + 1 for i in names]
    if names[0] not in range(numCols): names = ['"' + i + '"' for i in names]
    ranges = datajson[dataname]['ATTRS']['RANGE']
    ranges = [r.strip('(').strip(')').split(',') for r in ranges]
    ranges = [list(convertSeq(seq)) for seq in ranges]
    colValDict = dict(zip(names, ranges))
    
    colChoices = range(1, numCols + 1)
    rowChoices = range(1, numRows + 1)
    
    cols = '0' if random() < 0.5 else range(randrange(numCols))
    rows = '0' if random() < 0.5 else range(randrange(numRows)+1)
    
    #slice task 
    if FU == '[':
        cols = '0' if cols == '0' else [names[i] for i in cols]
        colPipe = '0' if random() < 0.5 else [choice(names) for x in range(randrange(numCols))]
        rowPipe = '0' if colPipe == '0'  else [choice(rowChoices) for x in range(randrange(numRows))]
        loopCols = '0' if random() < 0.5 else [choice(names) for x in range(randrange(numCols))]
        loopRows = '0' if random() < 0.5 else [choice(rowChoices) for x in range(randrange(numRows))]
        loopColPipe = '0' if random() < 0.5 else [choice(names) for x in range(randrange(numCols))]
        loopRowPipe = '0' if loopColPipe == '0' else [choice(rowChoices) for x in range(randrange(numRows))]
        cols = ';'.join(list(makeChar(cols)))
        rows = ';'.join(list(makeChar(rows)))
        colPipe = ';'.join(list(makeChar(colPipe)))
        rowPipe = ';'.join(list(makeChar(rowPipe)))
        colPipeRow = colPipe + '|' + rowPipe
        loopCols = ';'.join(list(makeChar(loopCols)))
        loopRows = ';'.join(list(makeChar(loopRows)))
        loopColPipe = ';'.join(list(makeChar(loopColPipe)))
        loopRowPipe = ';'.join(list(makeChar(loopRowPipe)))
        loopColPipeLoopRow = loopColPipe + '|' + loopRowPipe
        return [cols,rows,colPipeRow,loopCols,loopRows,loopColPipeLoopRow]
    
    #complex filter task
    if len(FU.split(';')) > 1:
        colLPipe = []; colRPipe = []; colL2Pipe = []; colR2Pipe = []
        valLPipe = []; valRPipe = []; valL2Pipe = []; valR2Pipe = []
        for i in range(choiceAs):
            #for each i, make two val/col pairs, a LHS pair and a RHS pair
            colKeyL = getCol(colValDict); colKeyL2 = getCol(colValDict)
            colKeyR = getCol(colValDict); colKeyR2 = getCol(colValDict)
            rangeValuesL = colValDict[colKeyL]
            rangeValuesR = colValDict[colKeyR]
            rangeValuesL2 = colValDict[colKeyL2]
            rangeValuesR2 = colValDict[colKeyR2]
            colLPipe.append(colKeyL); colRPipe.append(colKeyR)
            colL2Pipe.append(colKeyL2); colR2Pipe.append(colKeyR2)
            valLPipe.append(uniform(rangeValuesL[0], rangeValuesL[1]))
            valRPipe.append(uniform(rangeValuesR[0], rangeValuesR[1]))
            valL2Pipe.append(uniform(rangeValuesL2[0], rangeValuesL2[1]))
            valR2Pipe.append(uniform(rangeValuesR2[0], rangeValuesR2[1]))
        colLPipe = ';'.join(list(makeChar(colLPipe)))
        colRPipe = ';'.join(list(makeChar(colRPipe)))
        colL2Pipe = ';'.join(list(makeChar(colL2Pipe)))
        colR2Pipe = ';'.join(list(makeChar(colR2Pipe)))
        valLPipe = ';'.join(list(makeChar(valLPipe)))
        valRPipe = ';'.join(list(makeChar(valRPipe)))
        valL2Pipe = ';'.join(list(makeChar(valL2Pipe)))
        valR2Pipe = ';'.join(list(makeChar(valR2Pipe)))
        valLPipeColL = valLPipe + '|' + colLPipe
        valRPipeColR = valRPipe + '|' + colRPipe
        valL2PipeColL2 = valL2Pipe + '|' + colL2Pipe
        valR2PipeColR2 = valR2Pipe + '|' + colR2Pipe
        return [valLPipeColL, valRPipeColR, valL2PipeColL2, valR2PipeColR2]

    #simple filter task
    colPipe = []; colPipe2 = []
    valPipe = []; valPipe2 = []
    for i in range(randrange(2*numCols)):
        colKey = getCol(colValDict)
        rangeValues = colValDict[colKey]
        colPipe.append(colKey)
        valPipe.append(uniform(rangeValues[0], rangeValues[1]))
        colKey2 = getCol(colValDict)
        rangeValues2 = colValDict[colKey2]
        colPipe2.append(colKey2)
        valPipe2.append(uniform(rangeValues2[0], rangeValues2[1]))
    colPipe = ';'.join(list(makeChar(colPipe)))
    valPipe = ';'.join(list(makeChar(valPipe)))
    colPipe2 = ';'.join(list(makeChar(colPipe2)))
    valPipe2 = ';'.join(list(makeChar(valPipe2)))
    valPipeCol = valPipe + '|' + colPipe
    valPipeCol2 = valPipe2 + '|' + colPipe2
    return [valPipeCol, valPipeCol2]
