
import h2o, h2o_cmd, sys
import time, random, re
# Trying to share some functions useful for creating random exec expressions
# and executing them
# these lists are just included for example
if (1==0):
    zeroList = [
            'Result0 = 0',
    ]
    exprList = [
            'Result<n> = colSwap(<keyX>,<col1>,(<keyX>[2]==0 ? 54321 : 54321))',
            'Result<n> = <keyX>[<col1>]',
            'Result<n> = min(<keyX>[<col1>])',
            'Result<n> = max(<keyX>[<col1>]) + Result<n-1>',
            'Result<n> = mean(<keyX>[<col1>]) + Result<n-1>',
            'Result<n> = sum(<keyX>[<col1>]) + Result.hex',
        ]

def checkForBadFP(min_value):
    if 'Infinity' in str(min_value):
        raise Exception("Infinity in inspected min_value (proxy for scalar result) can't be good: %s" % str(min))
    if 'NaN' in str(min):
        raise Exception("NaN in inspected min_value (proxy for scalar result)  can't be good: %s" % str(min))

def checkScalarResult(resultInspect, resultKey):
    # make the common problems easier to debug
    h2o.verboseprint("checkScalarResult resultInspect:", h2o.dump_json(resultInspect))
    # FIX! HACK!..seems like there is inconsistency
    # if 'type' is one level down, throw away the first level
    # weird..it's a tuple, not a list? when the extra level of hier is there
    # this works:
    if type(resultInspect) is not dict:
        ### print "Trimming resultInspect hier."
        resultInspect0 = resultInspect[0]
    else:
        resultInspect0 = resultInspect

    emsg = None
    while(True):
        if 'type' not in resultInspect0:
            emsg = "'type' missing. Look at the json just printed"
            break 
        t = resultInspect0["type"]
        if t != 'parsed':
            emsg = resultKey + " 'type' is not 'parsed'. Look at the json just printed"
            break 

        if 'rows' not in resultInspect0:
            emsg = "Inspect response: 'rows' missing. Look at the json just printed"
            break 
        rows = resultInspect0["rows"]

        if 'cols' not in resultInspect0:
            emsg = "Inspect response: 'cols' missing. Look at the json just printed"
            break 
        cols = resultInspect0["cols"]

        break

    if emsg is not None:
        print "\nKey: '" + resultKey + "' being inspected:\n", h2o.dump_json(resultInspect)
        sys.stdout.flush()
        raise Exception("Inspect problem:" + emsg)

    # Cycle thru rows and extract all the meta-data into a dict?   
    # assume "0" and "row" keys exist for each list entry in rows
    # FIX! the key for the value can be 0 or 1 or ?? (apparently col?) Should change H2O here
    metaDict = cols[0]
    for key,value in metaDict.items():
        h2o.verboseprint("Inspect metadata:", key, value)
            
    min_value = metaDict['min']
    checkForBadFP(min_value)

    return min_value

def fill_in_expr_template(exprTemplate, colX, n, row, key2):
    # FIX! does this push col2 too far? past the output col?
    # just a string? 
    execExpr = exprTemplate
    execExpr = re.sub('<col1>',str(colX),execExpr)
    execExpr = re.sub('<col2>',str(colX+1),execExpr)
    execExpr = re.sub('<n>',str(n),execExpr)
    execExpr = re.sub('<n-1>',str(n-1),execExpr)
    execExpr = re.sub('<row>',str(row),execExpr)
    execExpr = re.sub('<keyX>',str(key2),execExpr)
    ### h2o.verboseprint("\nexecExpr:", execExpr)
    print "execExpr:", execExpr
    return execExpr


def exec_expr(node, execExpr, resultKey="Result.hex", timeoutSecs=10):
    start = time.time()
    # FIX! Exec has 'escape_nan' arg now. should we test?
    resultExec = h2o_cmd.runExecOnly(node, expression=execExpr, timeoutSecs=timeoutSecs, escape_nan=0)
    h2o.verboseprint(resultExec)
    h2o.verboseprint('exec took', time.time() - start, 'seconds')
    ### print 'exec took', time.time() - start, 'seconds'

    h2o.verboseprint("\nfirst look at the default Result key")
    # new offset=-1 to get the metadata?
    defaultInspectM1 = h2o_cmd.runInspect(None, "Result.hex", offset=-1)
    checkScalarResult(defaultInspectM1, "Result.hex")

    h2o.verboseprint("\nNow look at the assigned " + resultKey + " key")
    resultInspectM1 = h2o_cmd.runInspect(None, resultKey, offset=-1)
    min_value = checkScalarResult(resultInspectM1, resultKey)

    return resultInspectM1, min_value


def exec_zero_list(zeroList):
    # zero the list of Results using node[0]
    for exprTemplate in zeroList:
        execExpr = fill_in_expr_template(exprTemplate,0,0,0,"Result.hex")
        execResult = exec_expr(h2o.nodes[0], execExpr, "Result.hex")
        ### print "\nexecResult:", execResult


def exec_expr_list_rand(lenNodes, exprList, key2, 
    minCol=0, maxCol=54, minRow=1, maxRow=400000, maxTrials=200, timeoutSecs=10):

    trial = 0
    while trial < maxTrials: 
        exprTemplate = random.choice(exprList)

        # UPDATE: all execs are to a single node. No mixed node streams
        # eliminates some store/store race conditions that caused problems.
        # always go to node 0 (forever?)
        if lenNodes is None:
            execNode = 0
        else:
            # execNode = random.randint(0,lenNodes-1)
            execNode = 0
        ## print "execNode:", execNode

        colX = random.randint(minCol,maxCol)

        # FIX! should tune this for covtype20x vs 200x vs covtype.data..but for now
        row = str(random.randint(minRow,maxRow))

        execExpr = fill_in_expr_template(exprTemplate, colX, ((trial+1)%4)+1, row, key2)
        execResultInspect = exec_expr(h2o.nodes[execNode], execExpr, "Result.hex", timeoutSecs)
        ### print "\nexecResult:", execResultInspect

        checkScalarResult(execResultInspect, "Result.hex")

        sys.stdout.write('.')
        sys.stdout.flush()

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        # slows things down to check every iteration, but good for isolation
        if (h2o.check_sandbox_for_errors()):
            raise Exception(
                "Found errors in sandbox stdout or stderr, on trial #%s." % trial)
        trial += 1
        print "Trial #", trial, "completed\n"

def exec_expr_list_across_cols(lenNodes, exprList, key2, 
    minCol=0, maxCol=54, timeoutSecs=10, incrementingResult=True):
    colResultList = []
    for colX in range(minCol, maxCol):
        for exprTemplate in exprList:
            # do each expression at a random node, to facilate key movement
            # UPDATE: all execs are to a single node. No mixed node streams
            # eliminates some store/store race conditions that caused problems.
            # always go to node 0 (forever?)
            if lenNodes is None:
                execNode = 0
            else:
                ### execNode = random.randint(0,lenNodes-1)
                ### print execNode
                execNode = 0

            execExpr = fill_in_expr_template(exprTemplate, colX, colX, 0, key2)
            if incrementingResult: # the Result<col> pattern
                resultKey = "Result"+str(colX)
            else: # assume it's a re-assign to self
                resultKey = key2

            execResultInspect = exec_expr(h2o.nodes[execNode], execExpr,
                resultKey, timeoutSecs)
            ### print "\nexecResult:", execResultInspect

            # min is keyword. shouldn't use.
            if incrementingResult: # a col will have a single min
                min_value = checkScalarResult(execResultInspect, resultKey)
                h2o.verboseprint("min_value: ", min_value, "col:", colX)
                print "min_value: ", min_value, "col:", colX
            else:
                min_value = None

            sys.stdout.write('.')
            sys.stdout.flush()

            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
            # slows things down to check every iteration, but good for isolation
            if (h2o.check_sandbox_for_errors()):
                raise Exception(
                    "Found errors in sandbox stdout or stderr, on trial #%s." % trial)

        print "Column #", colX, "completed\n"
        colResultList.append(min_value)

    return colResultList


