import random, time
import h2o, h2o_cmd, h2o_gbm

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
        node = h2o.nodes[0]

    if 'warnings' in rfv:
        warnings = rfv['warnings']
        # catch the 'Failed to converge" for now
        for w in warnings:
            if not noPrint: print "\nwarning:", w
            if ('Failed' in w) or ('failed' in w):
                raise Exception(w)

    #****************************
    # v2 doesn't have the response variable, so removed looking at it
    if h2o.beta_features:
        # if we are checking after confusion_matrix for predict, the jsonschema is different
        if 'cm' in rfv:
            cm = rfv['cm'] # only one
        else:
            cms = rfv['drf_model']['cms']
            print "number of cms:", len(cms)
            print "FIX! need to add reporting of h2o's _perr per class error"
            # FIX! what if regression. is rf only classification?
            print "cms[-1]['_arr']:", cms[-1]['_arr']
            print "cms[-1]['_predErr']:", cms[-1]['_predErr']
            print "cms[-1]['_classErr']:", cms[-1]['_classErr']
            # print "cms[-1]:", h2o.dump_json(cms[-1])
            cm = cms[-1]['_arr'] # take the last one
        scoresList = cm

        if not checkScoringOnly:
            rf_model = rfv['drf_model']
            used_trees = rf_model['N']
            errs = rf_model['errs']
            print "errs[0]:", errs[0]
            print "errs[-1]:", errs[-1]
            print "errs:", errs
            # if we got the ntree for comparison. Not always there in kwargs though!
            param_ntrees = kwargs.get('ntrees',None)
            if (param_ntrees is not None and used_trees != param_ntrees):
                raise Exception("used_trees should == param_ntree. used_trees: %s"  % used_trees)
            if (used_trees+1)!=len(cms) or (used_trees+1)!=len(errs):
                raise Exception("len(cms): %s and len(errs): %s should be one more than N %s trees" % (len(cms), len(errs), N))

    else:
        cm = rfv['confusion_matrix']
        header = cm['header'] # list
        classification_error = cm['classification_error']
        rows_skipped = cm['rows_skipped']
        cm_type = cm['type']
        if not noPrint: 
            print "classification_error * 100 (pct):", classification_error * 100
            print "rows_skipped:", rows_skipped
            print "type:", cm_type
        scoresList = cm['scores'] # list
        used_trees = cm['used_trees']
        # if we got the ntree for comparison. Not always there in kwargs though!
        param_ntree = kwargs.get('ntree',None)
        if (param_ntree is not None and used_trees != param_ntree):
            raise Exception("used_trees should == param_ntree. used_trees: %s"  % used_trees)


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
        h2o.check_sandbox_for_errors()
        classification_error = pctWrong
        return (round(classification_error,2), classErrorPctList, totalScores)

    #****************************
    # more testing for RFView
    if (totalScores<=0 or totalScores>5e9):
        raise Exception("scores in RFView seems wrong. scores:", scoresList)

    if h2o.beta_features:
        varimp = rf_model['varimp']
        treeStats = rf_model['treeStats']
        data_key = rf_model['_dataKey']
        model_key = rf_model['_key']
        classification_error = pctWrong

        if not noPrint: 
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
    
    else:
        cmtype = cm['type']
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
        depth = trees['depth'] # Dict
        # zero depth okay?
        ## if ' 0.0 ' in depth:
        ##     raise Exception("depth in RFView seems wrong. depth:", depth)
        leaves = trees['leaves'] # Dict
        if ' 0.0 ' in leaves:
            raise Exception("leaves in RFView seems wrong. leaves:", leaves)

        if not noPrint: 
            print """
             Leaves: {0} / {1} / {2}
              Depth: {3} / {4} / {5}
               mtry: {6}
               Type: {7}
                Err: {8} %
            """.format(
                    rfv['trees']['leaves']['min'],
                    rfv['trees']['leaves']['mean'],
                    rfv['trees']['leaves']['max'],
                    rfv['trees']['depth']['min'],
                    rfv['trees']['depth']['mean'],
                    rfv['trees']['depth']['max'],
                    rfv['mtry'],
                    rfv['confusion_matrix']['type'],
                    rfv['confusion_matrix']['classification_error'] *100,
                    )
        
        number_built = trees['number_built']
        if (number_built<=0 or number_built>20000):
            raise Exception("number_built in RFView seems wrong. number_built:", number_built)

        h2o.verboseprint("RFView response: number_built:", number_built, "leaves:", leaves, "depth:", depth)

    ### modelInspect = node.inspect(model_key)
    dataInspect = node.inspect(data_key)
    h2o.check_sandbox_for_errors()
    return (round(classification_error,2), classErrorPctList, totalScores)

def simpleCheckRFScore(node=None, rfv=None, noPrint=False, **kwargs):
    simpleCheckRFView(node=node, rfv=rfv, noPrint=noPrint, checkScoringOnly=True, **kwargs)

def trainRF(trainParseResult, **kwargs):
    # Train RF
    start = time.time()
    trainResult = h2o_cmd.runRF(parseResult=trainParseResult, **kwargs)
    rftime      = time.time()-start 
    h2o.verboseprint("RF train results: ", trainResult)
    h2o.verboseprint("RF computation took {0} sec".format(rftime))

    trainResult['python_call_timer'] = rftime
    return trainResult

# vactual only needed for v2?
def scoreRF(scoreParseResult, trainResult, vactual=None, timeoutSecs=120, **kwargs):
    # Run validation on dataset

    parseKey = scoreParseResult['destination_key']
    if h2o.beta_features:
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

        predictCMResult = h2o.nodes[0].predict_confusion_matrix(
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

    else:
        ntree = trainResult['ntree']
        rfModelKey  = trainResult['model_key']
        start = time.time()
        # NOTE: response_variable is required, and passed from kwargs here
        # out_of_bag_error_estimate=0 is required for scoring. H2O will assert if 1 and different data set
        # compared to training
        kwargs['out_of_bag_error_estimate'] = 0
        scoreResult = h2o_cmd.runRFView(None, parseKey, rfModelKey, ntree=ntree, timeoutSecs=timeoutSecs, **kwargs)

    rftime      = time.time()-start 
    h2o.verboseprint("RF score results: ", scoreResult)
    h2o.verboseprint("RF computation took {0} sec".format(rftime))
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

