import random, time, csv
import h2o_cmd, h2o_gbm, h2o_exec as h2e, h2o_util
import h2o_nodes
from h2o_test import check_sandbox_for_errors, dump_json, verboseprint

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

        # can't have both
        if 'cols' in params and 'ignored_cols_by_name' in params:
            if params['cols'] and params['ignored_cols_by_name']:
                params['cols'] = None

        if (randomKey=='x'):
            colX = randomValue
        # temp hack to avoid CM=0 results if 100% sample and using OOBEE
        # UPDATE: if the default is oobe=1, it might not be in params...just have to not have the
        # test ask for 100
    return colX


def simpleCheckRFView(node=None, rfv=None, checkScoringOnly=False, noPrint=False, **kwargs):
    if not node:
        node = h2o_nodes.nodes[0]

    if 'warnings' in rfv:
        warnings = rfv['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            if not noPrint: print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

    #****************************
    # if we are checking after confusion_matrix for predict, the jsonschema is different

    if 'cm' in rfv:
        cm = rfv['cm'] # only one
    else:
        if 'drf_model' in rfv:
            rf_model = rfv['drf_model']
        elif 'speedrf_model' in rfv:
            rf_model = rfv['speedrf_model']
        elif 'rf_model' in rfv:
            rf_model = rfv['rf_model']
        else:
            raise Exception("no rf_model in rfv? %s" % dump_json(rfv))

        cms = rf_model['cms']
        print "number of cms:", len(cms)
        print "FIX! need to add reporting of h2o's _perr per class error"
        # FIX! what if regression. is rf only classification?
        print "cms[-1]['_arr']:", cms[-1]['_arr']
        print "cms[-1]['_predErr']:", cms[-1]['_predErr']
        print "cms[-1]['_classErr']:", cms[-1]['_classErr']

        ## print "cms[-1]:", dump_json(cms[-1])
        ## for i,c in enumerate(cms):
        ##    print "cm %s: %s" % (i, c['_arr'])

        cm = cms[-1]['_arr'] # take the last one

    scoresList = cm

    if not checkScoringOnly:
        used_trees = rf_model['N']
        errs = rf_model['errs']
        print "errs[0]:", errs[0]
        print "errs[-1]:", errs[-1]
        print "errs:", errs
        # if we got the ntree for comparison. Not always there in kwargs though!
        param_ntrees = kwargs.get('ntrees', None)
        if (param_ntrees is not None and used_trees != param_ntrees):
            raise Exception("used_trees should == param_ntree. used_trees: %s"  % used_trees)
        if (used_trees+1)!=len(cms) or (used_trees+1)!=len(errs):
            raise Exception("len(cms): %s and len(errs): %s should be one more than N %s trees" % (len(cms), len(errs), used_trees))


    #****************************
    totalScores = 0
    totalRight = 0
    # individual scores can be all 0 if nothing for that output class
    # due to sampling
    classErrorPctList = []
    predictedClassDict = {} # may be missing some? so need a dict?
    for classIndex,s in enumerate(scoresList):
        classSum = sum(s)
        if classSum == 0 :
            # why would the number of scores for a class be 0? does RF CM have entries for non-existent classes
            # in a range??..in any case, tolerate. (it shows up in test.py on poker100)
            if not noPrint: print "class:", classIndex, "classSum", classSum, "<- why 0?"
        else:
            # H2O should really give me this since it's in the browser, but it doesn't
            classRightPct = ((s[classIndex] + 0.0)/classSum) * 100
            totalRight += s[classIndex]
            classErrorPct = round(100 - classRightPct, 2)
            classErrorPctList.append(classErrorPct)
            ### print "s:", s, "classIndex:", classIndex
            if not noPrint: print "class:", classIndex, "classSum", classSum, "classErrorPct:", "%4.2f" % classErrorPct

            # gather info for prediction summary
            for pIndex,p in enumerate(s):
                if pIndex not in predictedClassDict:
                    predictedClassDict[pIndex] = p
                else:
                    predictedClassDict[pIndex] += p

        totalScores += classSum

    #****************************
    if not noPrint: 
        print "Predicted summary:"
        # FIX! Not sure why we weren't working with a list..hack with dict for now
        for predictedClass,p in predictedClassDict.items():
            print str(predictedClass)+":", p

        # this should equal the num rows in the dataset if full scoring? (minus any NAs)
        print "totalScores:", totalScores
        print "totalRight:", totalRight
        if totalScores != 0:  
            pctRight = 100.0 * totalRight/totalScores
        else: 
            pctRight = 0.0
        pctWrong = 100 - pctRight
        print "pctRight:", "%5.2f" % pctRight
        print "pctWrong:", "%5.2f" % pctWrong

    if checkScoringOnly:
        check_sandbox_for_errors()
        classification_error = pctWrong
        return (round(classification_error,2), classErrorPctList, totalScores)

    # it's legal to get 0's for oobe error # if sample_rate = 1
    sample_rate = kwargs.get('sample_rate', None)
    validation = kwargs.get('validation', None)
    print "kevin:", sample_rate, validation
    if (sample_rate==1 and not validation): 
        pass
    elif (totalScores<=0 or totalScores>5e9):
        raise Exception("scores in RFView seems wrong. scores:", scoresList)

    varimp = rf_model['varimp']

    if 'importance' in kwargs and kwargs['importance']:
        max_var = varimp['max_var']
        variables = varimp['variables']
        varimpSD = varimp['varimpSD']
        varimp2 = varimp['varimp']

        # what is max_var? it's 100 while the length of the others is 54 for covtype
        if not max_var:
            raise Exception("varimp.max_var is None? %s" % max_var)
        # if not variables:
        #     raise Exception("varimp.variables is None? %s" % variables)
        if not varimpSD:
            raise Exception("varimp.varimpSD is None? %s" % varimpSD)
        if not varimp2:
            raise Exception("varimp.varimp is None? %s" % varimp2)

        # check that they all have the same length and that the importance is not all zero
        # if len(varimpSD)!=max_var or len(varimp2)!=max_var or len(variables)!=max_var:
        #    raise Exception("varimp lists seem to be wrong length: %s %s %s" % \
        #        (max_var, len(varimpSD), len(varimp2), len(variables)))

        # not checking maxvar or variables. Don't know what they should be
        if len(varimpSD) != len(varimp2):
            raise Exception("varimp lists seem to be wrong length: %s %s" % \
                (len(varimpSD), len(varimp2)))

        h2o_util.assertApproxEqual(sum(varimp2), 0.0, tol=1e-5, 
            msg="Shouldn't have all 0's in varimp %s" % varimp2)

    treeStats = rf_model['treeStats']
    if not treeStats:
        raise Exception("treeStats not right?: %s" % dump_json(treeStats))
    # print "json:", dump_json(rfv)
    data_key = rf_model['_dataKey']
    model_key = rf_model['_key']
    classification_error = pctWrong

    if not noPrint: 
        if 'minLeaves' not in treeStats or not treeStats['minLeaves']:
            raise Exception("treeStats seems to be missing minLeaves %s" % dump_json(treeStats))
        print """
         Leaves: {0} / {1} / {2}
          Depth: {3} / {4} / {5}
            Err: {6:0.2f} %
        """.format(
                treeStats['minLeaves'],
                treeStats['meanLeaves'],
                treeStats['maxLeaves'],
                treeStats['minDepth'],
                treeStats['meanDepth'],
                treeStats['maxDepth'],
                classification_error,
                )
    
    ### modelInspect = node.inspect(model_key)
    dataInspect = h2o_cmd.runInspect(key=data_key)
    check_sandbox_for_errors()
    return (round(classification_error,2), classErrorPctList, totalScores)

def simpleCheckRFScore(node=None, rfv=None, noPrint=False, **kwargs):
    (classification_error, classErrorPctList, totalScores) = simpleCheckRFView(node=node, rfv=rfv, 
        noPrint=noPrint, checkScoringOnly=True, **kwargs)
    return (classification_error, classErrorPctList, totalScores)

def trainRF(trainParseResult, scoreParseResult=None, **kwargs):
    # Train RF
    start = time.time()

    if scoreParseResult:
        trainResult = h2o_cmd.runRF(
            parseResult=trainParseResult, 
            validation=scoreParseResult['destination_key'],
            **kwargs)
    else:
        trainResult = h2o_cmd.runRF(
           parseResult=trainParseResult, 
           **kwargs)

    rftime      = time.time()-start 
    verboseprint("RF train results: ", trainResult)
    verboseprint("RF computation took {0} sec".format(rftime))

    trainResult['python_call_timer'] = rftime
    return trainResult

# vactual only needed for v2?
def scoreRF(scoreParseResult, trainResult, vactual=None, timeoutSecs=120, **kwargs):
    # Run validation on dataset

    parseKey = scoreParseResult['destination_key']
    # this is how we're supposed to do scorin?
    rfModelKey  = trainResult['drf_model']['_key']
    predictKey = 'Predict.hex'
    start = time.time()
    predictResult = h2o_cmd.runPredict(
        data_key=parseKey,
        model_key=rfModelKey,
        destination_key=predictKey,
        timeoutSecs=timeoutSecs, **kwargs)

    h2o_cmd.runInspect(key='Predict.hex', verbose=True)

    predictCMResult = h2o_nodes.nodes[0].predict_confusion_matrix(
        actual=parseKey,
        vactual=vactual,
        predict=predictKey,
        vpredict='predict', 
        timeoutSecs=timeoutSecs, **kwargs)
        
    rftime      = time.time()-start 

    cm = predictCMResult['cm']

    # These will move into the h2o_gbm.py
    pctWrong = h2o_gbm.pp_cm_summary(cm);
    print "\nTest\n==========\n"
    print h2o_gbm.pp_cm(cm)
    scoreResult = predictCMResult

    rftime      = time.time()-start 
    verboseprint("RF score results: ", scoreResult)
    verboseprint("RF computation took {0} sec".format(rftime))
    scoreResult['python_call_timer'] = rftime
    return scoreResult

# this is only for v1. simpleCheckRFView should be fine/equivalent for v1 + v2?
def pp_rf_result(rf):
    jcm = rf['confusion_matrix']
    header = jcm['header']

    cm = '{0:<15}'.format('')
    for h in header: cm = '{0}|{1:<15}'.format(cm, h)

    cm = '{0}|{1:<15}'.format(cm, 'error')
    c = 0
    for line in jcm['scores']:
        lineSum  = sum(line)
        errorSum = lineSum - line[c]
        if (lineSum>0): 
            err = float(errorSum) / lineSum
        else:
            err = 0.0
        fl = '{0:<15}'.format(header[c])
        for num in line: fl = '{0}|{1:<15}'.format(fl, num)
        fl = '{0}|{1:<15}'.format(fl, err)
        cm = "{0}\n{1}".format(cm, fl)
        c += 1

    return """
 Leaves: {0} / {1} / {2}
  Depth: {3} / {4} / {5}
   mtry: {6}
   Type: {7}
    Err: {8} %
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
        cm)

#************************************************************************
# used these for comparing predict output to csv file, to actual (use csv reader
#************************************************************************
# translate provides the mapping between original and predicted
def compare_csv_at_one_col(csvPathname, msg, colIndex=-1, translate=None, skipHeader=0):
    predictOutput = []
    with open(csvPathname, 'rb') as f:
        reader = csv.reader(f)
        print "csv read of", csvPathname, "column", colIndex
        rowNum = 0
        for row in reader:
            # print the last col
            # ignore the first row ..header
            if skipHeader==1 and rowNum==0:
                print "Skipping header in this csv"
            else:
                output = row[colIndex]
                if translate:
                    output = translate[output]
                # only print first 10 for seeing
                if rowNum<10: print msg, "raw output col:", row[colIndex], "translated:", output
                predictOutput.append(output)
            rowNum += 1
    return (rowNum, predictOutput)

def predict_and_compare_csvs(model_key, hex_key, predictHexKey, 
    csvSrcOutputPathname, csvPredictPathname, 
    skipSrcOutputHeader, skipPredictHeader,
    translate=None, y=0):
    # have to slice out col 0 (the output) and feed result to predict
    # cols are 0:784 (1 output plus 784 input features
    # h2e.exec_expr(execExpr="P.hex="+hex_key+"[1:784]", timeoutSecs=30)
    dataKey = "P.hex"
    h2e.exec_expr(execExpr=dataKey+"="+hex_key, timeoutSecs=30) # unneeded but interesting
    if skipSrcOutputHeader:
        print "Has header in dataset, so should be able to chop out col 0 for predict and get right answer"
        print "hack for now, can't chop out col 0 in Exec currently"
        dataKey = hex_key
    else:
        print "No header in dataset, can't chop out cols, since col numbers are used for names"
        dataKey = hex_key

    # +1 col index because R-like
    h2e.exec_expr(execExpr="Z.hex="+hex_key+"[,"+str(y+1)+"]", timeoutSecs=30)

    start = time.time()
    predict = h2o_nodes.nodes[0].generate_predictions(model_key=model_key,
        data_key=hex_key, destination_key=predictHexKey)
    print "generate_predictions end on ", hex_key, " took", time.time() - start, 'seconds'
    check_sandbox_for_errors()
    inspect = h2o_cmd.runInspect(key=predictHexKey)
    h2o_cmd.infoFromInspect(inspect, 'predict.hex')

    h2o_nodes.nodes[0].csv_download(src_key="Z.hex", csvPathname=csvSrcOutputPathname)
    h2o_nodes.nodes[0].csv_download(src_key=predictHexKey, csvPathname=csvPredictPathname)
    check_sandbox_for_errors()

    print "Do a check of the original output col against predicted output"
    (rowNum1, originalOutput) = compare_csv_at_one_col(csvSrcOutputPathname,
        msg="Original", colIndex=0, translate=translate, skipHeader=skipSrcOutputHeader)
    (rowNum2, predictOutput)  = compare_csv_at_one_col(csvPredictPathname,
        msg="Predicted", colIndex=0, skipHeader=skipPredictHeader)

    # no header on source
    if ((rowNum1-skipSrcOutputHeader) != (rowNum2-skipPredictHeader)):
        raise Exception("original rowNum1: %s - %d not same as downloaded predict: rowNum2: %s - %d \
            %s" % (rowNum1, skipSrcOutputHeader, rowNum2, skipPredictHeader))

    wrong = 0
    for rowNum,(o,p) in enumerate(zip(originalOutput, predictOutput)):
        # if float(o)!=float(p):
        if str(o)!=str(p):
            if wrong==10:
                print "Not printing any more mismatches\n"
            elif wrong<10:
                msg = "Comparing original output col vs predicted. row %s differs. \
                    original: %s predicted: %s"  % (rowNum, o, p)
                print msg
            wrong += 1

    print "\nTotal wrong:", wrong
    print "Total:", len(originalOutput)
    pctWrong = (100.0 * wrong)/len(originalOutput)
    print "wrong/Total * 100 ", pctWrong
    return pctWrong
