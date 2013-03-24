import h2o
import h2o_cmd
import random
import time

# params is mutable here
def pickRandRfParams(paramDict, params):
    colX = 0
    randomGroupSize = random.randint(1,len(paramDict))
    for i in range(randomGroupSize):
        randomKey = random.choice(paramDict.keys())
        randomV = paramDict[randomKey]
        randomValue = random.choice(randomV)
        # note it updates params, so any default values will still be there
        params[randomKey] = randomValue
        if (randomKey=='x'):
            colX = randomValue
        # temp hack to avoid CM=0 results if 100% sample and using OOBEE
        # UPDATE: if the default is oobe=1, it might not be in params...just have to not have the
        # test ask for 100
    return colX

def simpleCheckRFView(node, rfv, noprint=False, **kwargs):
    if not node:
        node = h2o.nodes[0]

    if 'warnings' in rfv:
        warnings = rfv['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            if not noprint: print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

    oclass = rfv['response_variable']
    if (oclass<0 or oclass>20000):
        raise Exception("class in RFView seems wrong. class:", oclass)

    # the simple assigns will at least check the key exists
    cm = rfv['confusion_matrix']
    header = cm['header'] # list
    classification_error = cm['classification_error']
    rows_skipped = cm['rows_skipped']
    cm_type = cm['type']
    if not noprint: 
        print "classification_error * 100 (pct):", classification_error * 100
        print "rows_skipped:", rows_skipped
        print "type:", cm_type

    used_trees = cm['used_trees']
    if (used_trees <= 0):
        raise Exception("used_trees should be >0. used_trees:", used_trees)

    # if we got the ntree for comparison. Not always there in kwargs though!
    ntree = kwargs.get('ntree',None)
    if (ntree is not None and used_trees != ntree):
        raise Exception("used_trees should == ntree. used_trees:", used_trees)

    scoresList = cm['scores'] # list
    totalScores = 0
    # individual scores can be all 0 if nothing for that output class
    # due to sampling
    classErrorPctList = []
    for classIndex,s in enumerate(scoresList):
        classSum = sum(s)
        if classSum == 0 :
            # why would the number of scores for a class be 0? does RF CM have entries for non-existent classes
            # in a range??..in any case, tolerate. (it shows up in test.py on poker100)
            if not noprint: print "class:", classIndex, "classSum", classSum, "<- why 0?"
        else:
            # H2O should really give me this since it's in the browser, but it doesn't
            classRightPct = ((s[classIndex] + 0.0)/classSum) * 100
            classErrorPct = 100 - classRightPct
            classErrorPctList.append(classErrorPct)
            ### print "s:", s, "classIndex:", classIndex
            if not noprint: print "class:", classIndex, "classSum", classSum, "classErrorPct:", "%4.2f" % classErrorPct
        totalScores += classSum

    # this should equal the num rows in the dataset if full scoring? (minus any NAs)
    if not noprint: print "totalScores:", totalScores

    if (totalScores<=0 or totalScores>5e9):
        raise Exception("scores in RFView seems wrong. scores:", scoresList)

    type = cm['type']
    used_trees = cm['used_trees']
    if (used_trees<=0 or used_trees>20000):
        raise Exception("used_trees in RFView seems wrong. used_trees:", used_trees)

    data_key = rfv['data_key']
    model_key = rfv['model_key']
    ntree = rfv['ntree']
    if (ntree<=0 or ntree>20000):
        raise Exception("ntree in RFView seems wrong. ntree:", ntree)
    response = rfv['response'] # Dict
    rfv_h2o = response['h2o']
    rfv_node = response['node']
    status = response['status']
    time = response['time']

    trees = rfv['trees'] # Dict
    depth = trees['depth']
    # zero depth okay?
    ## if ' 0.0 ' in depth:
    ##     raise Exception("depth in RFView seems wrong. depth:", depth)
    leaves = trees['leaves']
    if ' 0.0 ' in leaves:
        raise Exception("leaves in RFView seems wrong. leaves:", leaves)
    number_built = trees['number_built']
    if (number_built<=0 or number_built>20000):
        raise Exception("number_built in RFView seems wrong. number_built:", number_built)

    h2o.verboseprint("RFView response: number_built:", number_built, "leaves:", leaves, "depth:", depth)

    # just touching these keys to make sure they're good?
    confusion_key = rfv['confusion_key']

    # 2/14/13 kbn. can we not model any more? causes assertion error
    ### confusionInspect = node.inspect(confusion_key)
    ### modelInspect = node.inspect(model_key)
    dataInspect = node.inspect(data_key)

    return (classification_error, classErrorPctList, totalScores)

def trainRF(trainParseKey, **kwargs):
    # Train RF
    start = time.time()
    trainResult = h2o_cmd.runRFOnly(parseKey=trainParseKey, **kwargs)
    rftime      = time.time()-start 
    h2o.verboseprint("RF train results: ", trainResult)
    h2o.verboseprint("RF computation took {0} sec".format(rftime))

    trainResult['python_call_timer'] = rftime
    return trainResult

def scoreRF(scoreParseKey, trainResult, **kwargs):
    # Run validation on dataset
    rfModelKey  = trainResult['model_key']
    ntree       = trainResult['ntree']
    
    start = time.time()
    data_key = scoreParseKey['destination_key']
    scoreResult = h2o_cmd.runRFView(None, data_key, rfModelKey, ntree, **kwargs)

    rftime      = time.time()-start 
    h2o.verboseprint("RF score results: ", scoreResult)
    h2o.verboseprint("RF computation took {0} sec".format(rftime))

    scoreResult['python_call_timer'] = rftime
    return scoreResult

def pp_rf_result(rf):
    jcm = rf['confusion_matrix']
    header = jcm['header']
    cm = ' '.join(header)
    c = 0
    for line in jcm['scores']:
        lineSum  = sum(line)
        errorSum = lineSum - line[c]
        if (lineSum>0): 
            err = float(errorSum) / lineSum
        else:
            err = 0.0
        cm = "{0}\n {1} {2} {3}".format(cm, header[c], ' '.join(map(str,line)), err)
        c += 1

    return """
 Leaves: {0} / {1} / {2}
  Depth: {3} / {4} / {5}
   mtry: {6}
   Type: {7}
    Err: {8} %
   Time: {9} seconds

   Confusion matrix:
      {10}
""".format(
        rf['trees']['leaves']['min'],
        rf['trees']['leaves']['mean'],
        rf['trees']['leaves']['max'],
        rf['trees']['depth']['min'],
        rf['trees']['depth']['mean'],
        rf['trees']['depth']['max'],
        rf['mtry'], 
        rf['confusion_matrix']['type'],
        rf['confusion_matrix']['classification_error'] *100,
        rf['response']['time'],
        cm)

