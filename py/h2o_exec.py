
import h2o, h2o_cmd, sys
import time, random, re
import h2o_browse as h2b

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
    resultInspect0 = resultInspect[0]

    if 'num_rows' not in resultInspect0:
        emsg = "Inspect response: 'num_rows' missing. Look at the json just printed"
    elif 'cols' not in resultInspect0:
        emsg = "Inspect response: 'cols' missing. Look at the json just printed"
    else:
        emsg = None
        num_cols = resultInspect0["num_cols"]
        num_rows = resultInspect0["num_rows"]
        cols = resultInspect0["cols"]
        # print "cols:", h2o.dump_json(cols)

    if emsg:
        print "\nKey: '" + str(resultKey) + "' inspect result:\n", h2o.dump_json(resultInspect)
        sys.stdout.flush()
        raise Exception("Inspect problem:" + emsg)

    # Cycle thru rows and extract all the meta-data into a dict?   
    # assume "0" and "row" keys exist for each list entry in rows
    # FIX! the key for the value can be 0 or 1 or ?? (apparently col?) Should change H2O here

    # cols may not exist..if the result was just scalar?
    if not cols:
        # raise Exception("cols is null: %s" % cols)
        # just return the scalar result then
        scalar = resultInspect0['scalar']
        if scalar is None:
            raise Exception("both cols and scalar are null: %s %s" % (cols, scalar))
        checkForBadFP(scalar)
        return scalar

    metaDict = cols[0]
    for key,value in metaDict.items():
        print "Inspect metaDict:", key, value
            
    min_value = metaDict['min']
    checkForBadFP(min_value)


    # do a VA inspect to see if the fvec to va converter works
    return min_value

def fill_in_expr_template(exprTemplate, colX=None, n=None, row=None, keyX=None, m=None):
    # FIX! does this push col2 too far? past the output col?
    # just a string? 
    execExpr = exprTemplate
    if colX is not None:
        print "Assume colX %s is zero-based..added 1 for R based exec2" % colX
        execExpr = re.sub('<col1>', str(colX+1), execExpr)
        # this is just another value
        execExpr = re.sub('<col2>', str(colX+2), execExpr)
    if n is not None:
        execExpr = re.sub('<n>', str(n), execExpr)
        execExpr = re.sub('<n-1>', str(n-1), execExpr)
    if row is not None:
        execExpr = re.sub('<row>', str(row), execExpr)
    if keyX is not None:
        execExpr = re.sub('<keyX>', str(keyX), execExpr)
    if m is not None:
        execExpr = re.sub('<m>', str(m), execExpr)
        execExpr = re.sub('<m-1>', str(m-1), execExpr)
    ### h2o.verboseprint("\nexecExpr:", execExpr)
    print "execExpr:", execExpr
    return execExpr


def exec_expr(node=None, execExpr=None, resultKey=None, timeoutSecs=10, ignoreH2oError=False):
    if not node:
        node = h2o.nodes[0]
    start = time.time()
    # FIX! Exec has 'escape_nan' arg now. should we test?
    # 5/14/13 removed escape_nan=0

    kwargs = {'str': execExpr} 
    resultExec = h2o_cmd.runExec(node, timeoutSecs=timeoutSecs, ignoreH2oError=ignoreH2oError, **kwargs)
    h2o.verboseprint('exec took', time.time() - start, 'seconds')
    h2o.verboseprint(resultExec)

    if 'cols' in resultExec and resultExec['cols']: # not null
        if 'funstr' in resultExec and resultExec['funstr']: # not null
            raise Exception("cols and funstr shouldn't both be in resultExec: %s" % h2o.dump_json(resultExec))
        else:
            # Frame
            # if test said to look at a resultKey, it's should be in h2o k/v store
            # inspect a result key?
            if resultKey is not None:
                kwargs = {'str': resultKey} 
                resultExec = h2o_cmd.runExec(node, timeoutSecs=timeoutSecs, ignoreH2oError=ignoreH2oError, **kwargs)
                h2o.verboseprint("resultExec2:", h2o.dump_json(resultExec))

            # handles the 1x1 data frame result. Not really interesting if bigger than 1x1?
            result = resultExec['cols'][0]['min']
        
    else: 
        if 'funstr' in resultExec and resultExec['funstr']: # not null
            # function return 
            result = resultExec['funstr']
        else:
            # scalar
            result = resultExec['scalar']
            
    return resultExec, result



def exec_zero_list(zeroList):
    # zero the list of Results using node[0]
    for exprTemplate in zeroList:
        execExpr = fill_in_expr_template(exprTemplate,0, 0, 0, None)
        execResult = exec_expr(h2o.nodes[0], execExpr, None)
        ### print "\nexecResult:", execResult


def exec_expr_list_rand(lenNodes, exprList, keyX, 
    # exec2 uses R "start with 1" behavior?
    minCol=1, maxCol=55, 
    minRow=1, maxRow=400000, 
    maxTrials=200, 
    timeoutSecs=10, ignoreH2oError=False):

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

        execExpr = fill_in_expr_template(exprTemplate, colX, ((trial+1)%4)+1, row, keyX)
        execResultInspect = exec_expr(h2o.nodes[execNode], execExpr, None, 
            timeoutSecs, ignoreH2oError)
        ### print "\nexecResult:", execResultInspect

        checkScalarResult(execResultInspect, None)

        # assume keyX is the lhs, and do a VA inspect of keyX
        if keyX:
            vaInspect = h2o.nodes[0].inspect(key=keyX, useVA=True)
        # print "va Inspect:", h2o.dump_json(vaInspect)
        # checkScalarResult(vaInspect, keyX)

        sys.stdout.write('.')
        sys.stdout.flush()

        ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
        # slows things down to check every iteration, but good for isolation
        if (h2o.check_sandbox_for_errors()):
            raise Exception(
                "Found errors in sandbox stdout or stderr, on trial #%s." % trial)
        trial += 1
        print "Trial #", trial, "completed\n"

def exec_expr_list_across_cols(lenNodes, exprList, keyX, 
    minCol=0, maxCol=54, timeoutSecs=10, incrementingResult=True):
    colResultList = []
    for colX in range(minCol, maxCol):
        for i, exprTemplate in enumerate(exprList):

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

            execExpr = fill_in_expr_template(exprTemplate, colX, colX, 0, keyX)
            if incrementingResult: # the Result<col> pattern
                resultKey = "Result"+str(colX)
            else: # assume it's a re-assign to self
                resultKey = keyX

            # kbn

            # v1
            # execResultInspect = exec_expr(h2o.nodes[execNode], execExpr, resultKey, timeoutSecs)
            # v2
            execResultInspect = exec_expr(h2o.nodes[execNode], execExpr, None, timeoutSecs)
            print "\nexecResult:", h2o.dump_json(execResultInspect)
            execResultKey = execResultInspect[0]['key']

            # v2: Exec2 'apply' can have no key field? (null) maybe just use keyX then
            if execResultKey:
                resultInspect = h2o_cmd.runInspect(None, execResultKey)
            else:
                resultInspect = h2o_cmd.runInspect(None, keyX)
            ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")

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


