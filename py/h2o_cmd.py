import os, json, unittest, time, shutil, sys, socket
import h2o_browse as h2b, h2o_rf as h2f, h2o_exec, h2o_gbm, h2o_util

import h2o_nodes
from h2o_test import dump_json, check_sandbox_for_errors, verboseprint

# print "h2o_cmd"

def parseS3File(node=None, bucket=None, filename=None, keyForParseResult=None, 
    timeoutSecs=20, retryDelaySecs=2, pollTimeoutSecs=30, **kwargs):
    ''' Parse a file stored in S3 bucket'''                                                                                                                                                                       
    if not bucket  : raise Exception('No S3 bucket')
    if not filename: raise Exception('No filename in bucket')
    if not node: node = h2o_nodes.nodes[0]
    
    import_result = node.import_s3(bucket)
    s3_key = [f['key'] for f in import_result['succeeded'] if f['file'] == filename ][0]
    
    if keyForParseResult is None:
        myKeyForParseResult = s3_key + '.hex'
    else:
        myKeyForParseResult = keyForParseResult
    p = node.parse(s3_key, myKeyForParseResult, 
        timeoutSecs, retryDelaySecs, 
        pollTimeoutSecs=pollTimeoutSecs, **kwargs)

    # do SummaryPage here too, just to get some coverage
    node.summary_page(myKeyForParseResult)
    return p

# normally we don't want inspect to print during verboseprint. verbose=True to get it
# in specific tests
def runInspect(node=None, key=None, timeoutSecs=30, verbose=False, **kwargs):
    if not key: raise Exception('No key for Inspect')
    if not node: node = h2o_nodes.nodes[0]
    a = node.inspect(key, timeoutSecs=timeoutSecs, **kwargs)
    if verbose:
        print "inspect of %s:" % key, dump_json(a)
    return a

def runSummary(node=None, key=None, timeoutSecs=30, **kwargs):
    if not key: raise Exception('No key for Summary')
    if not node: node = h2o_nodes.nodes[0]
    return node.summary_page(key, timeoutSecs=timeoutSecs, **kwargs)

# since we'll be doing lots of execs on a parsed file, not useful to have parse+exec
# retryDelaySecs isn't used, 
def runExec(node=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    a = node.exec_query(timeoutSecs, **kwargs)
    check_sandbox_for_errors()
    return a

def runKMeans(node=None, parseResult=None, timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseResult: raise Exception('No parseResult for KMeans')
    if not node: node = h2o_nodes.nodes[0]
    return node.kmeans(parseResult['destination_key'], None, timeoutSecs, retryDelaySecs, **kwargs)

def runGLM(node=None, parseResult=None, timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseResult: raise Exception('No parseResult for GLM')
    if not node: node = h2o_nodes.nodes[0]
    return node.GLM(parseResult['destination_key'], 
        timeoutSecs, retryDelaySecs, **kwargs)

def runGLMScore(node=None, key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    return node.GLMScore(key, model_key, timeoutSecs, **kwargs)

def runGLMGrid(node=None, parseResult=None, timeoutSecs=60, retryDelaySecs=2, **kwargs):
    if not parseResult: raise Exception('No parseResult for GLMGrid')
    if not node: node = h2o_nodes.nodes[0]
    # no such thing as GLMGridView..don't use retryDelaySecs
    return node.GLMGrid(parseResult['destination_key'], timeoutSecs, **kwargs)

def runPCA(node=None, parseResult=None, timeoutSecs=600, **kwargs):
    if not parseResult: raise Exception('No parseResult for PCA')
    if not node: node = h2o_nodes.nodes[0]
    data_key = parseResult['destination_key']
    return node.pca(data_key=data_key, timeoutSecs=timeoutSecs, **kwargs)

def runNNetScore(node=None, key=None, model=None, timeoutSecs=600, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    return node.neural_net_score(key, model, timeoutSecs=timeoutSecs, **kwargs)

def runNNet(node=None, parseResult=None, timeoutSecs=600, **kwargs):
    if not parseResult: raise Exception('No parseResult for Neural Net')
    if not node: node = h2o_nodes.nodes[0]
    data_key = parseResult['destination_key']
    return node.neural_net(data_key=data_key, timeoutSecs=timeoutSecs, **kwargs)

def runDeepLearning(node=None, parseResult=None, timeoutSecs=600, **kwargs):
    if not parseResult: raise Exception('No parseResult for Deep Learning')
    if not node: node = h2o_nodes.nodes[0]
    data_key = parseResult['destination_key']
    return node.deep_learning(data_key=data_key, timeoutSecs=timeoutSecs, **kwargs)

def runGBM(node=None, parseResult=None, timeoutSecs=500, **kwargs):
    if not parseResult: raise Exception('No parseResult for GBM')
    if not node: node = h2o_nodes.nodes[0]
    data_key = parseResult['destination_key']
    return node.gbm(data_key=data_key, timeoutSecs=timeoutSecs, **kwargs) 

def runPredict(node=None, data_key=None, model_key=None, timeoutSecs=500, **kwargs):
    if not data_key: raise Exception('No data_key for run Predict')
    if not node: node = h2o_nodes.nodes[0]
    return node.generate_predictions(data_key, model_key, timeoutSecs=timeoutSecs,**kwargs) 

def runSpeeDRF(node=None, parseResult=None, ntrees=5, max_depth=10, timeoutSecs=20, **kwargs):
    if not parseResult: raise Exception("No parseResult for SpeeDRF")
    if not node: node = h2o_nodes.nodes[0]
    Key = parseResult['destination_key']
    return node.speedrf(Key, ntrees=ntrees, max_depth=max_depth, timeoutSecs=timeoutSecs, **kwargs)

def runSpeeDRFView(node=None, modelKey=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    return node.speedrf_view(modelKey=modelKey, timeoutSecs=timeoutSecs, **kwargs)

# rfView can be used to skip the rf completion view
# for creating multiple rf jobs
def runRF(node=None, parseResult=None, trees=5, timeoutSecs=20, **kwargs):
    if not parseResult: raise Exception('No parseResult for RF')
    if not node: node = h2o_nodes.nodes[0]
    Key = parseResult['destination_key']
    return node.random_forest(Key, trees, timeoutSecs, **kwargs)

def runRFTreeView(node=None, n=None, data_key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    return node.random_forest_treeview(n, data_key, model_key, timeoutSecs, **kwargs)

def runGBMView(node=None, model_key=None, timeoutSecs=300, retryDelaySecs=2, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    if not model_key: 
        raise Exception("\nNo model_key was supplied to the gbm view!")
    gbmView = node.gbm_view(model_key, timeoutSecs=timeoutSecs)
    return gbmView

def runNeuralView(node=None, model_key=None, timeoutSecs=300, retryDelaySecs=2, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    if not model_key: 
        raise Exception("\nNo model_key was supplied to the neural view!")
    neuralView = node.neural_view(model_key, timeoutSecs=timeoutSecs, retryDelaysSecs=retryDelaysecs)
    return neuralView

def runPCAView(node=None, modelKey=None, timeoutSecs=300, retryDelaySecs=2, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    if not modelKey:
        raise Exception("\nNo modelKey was supplied to the pca view!")
    pcaView = node.pca_view(modelKey, timeoutSecs=timeoutSecs)
    return pcaView

def runGLMView(node=None, modelKey=None, timeoutSecs=300, retryDelaySecs=2, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    if not modelKey:
        raise Exception("\nNo modelKey was supplied to the glm view!")
    glmView = node.glm_view(modelKey,timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
    return glmView

def runRFView(node=None, data_key=None, model_key=None, ntree=None, 
    timeoutSecs=15, retryDelaySecs=2, doSimpleCheck=True,
    noPrint=False, **kwargs):
    if not node: node = h2o_nodes.nodes[0]

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_view(data_key, model_key, ntree=ntree, timeoutSecs=timeoutSecs, **kwargs)
    if doSimpleCheck:
        h2f.simpleCheckRFView(node, rfView, noPrint=noPrint)
    return rfView

def runStoreView(node=None, timeoutSecs=30, noPrint=None, **kwargs):
    if not node: node = h2o_nodes.nodes[0]
    storeView = node.store_view(timeoutSecs, **kwargs)
    if not noPrint:
        for s in storeView['keys']:
            print "StoreView: key:", s['key']
            if 'rows' in s: 
                verboseprint("StoreView: rows:", s['rows'], "value_size_bytes:", s['value_size_bytes'])
    print node, 'storeView has', len(storeView['keys']), 'keys'
    return storeView

def port_live(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip,port))
        s.shutdown(2)
        return True
    except:
        return False

def wait_for_live_port(ip, port, retries=3):
    verboseprint("Waiting for {0}:{1} {2}times...".format(ip,port,retries))
    if not port_live(ip,port):
        count = 0
        while count < retries:
            if port_live(ip,port):
                count += 1
            else:
                count = 0
            time.sleep(1)
            dot()
    if not port_live(ip,port):
        raise Exception("[h2o_cmd] Error waiting for {0}:{1} {2}times...".format(ip,port,retries))


# checks the key distribution in the cloud, and prints warning if delta against avg
# is > expected
def checkKeyDistribution():
    c = h2o_nodes.nodes[0].get_cloud()
    nodes = c['nodes']
    print "Key distribution post parse, should be balanced"
    # get average
    totalKeys = 0
    for n in nodes:
        totalKeys += int(n['num_keys'])
    avgKeys = (totalKeys + 0.0)/len(nodes)
    # if more than 5% difference from average, print warning
    for n in nodes:
        print 'num_keys:', n['num_keys'], 'value_size_bytes:', n['value_size_bytes'],\
            'name:', n['name']
        delta = (abs(avgKeys - int(n['num_keys']))/avgKeys)
        if delta > 0.10:
            print "WARNING. avgKeys:", avgKeys, "and n['num_keys']:", n['num_keys'], "have >", "%.1f" % (100 * delta), "% delta"

# I use these in testdir_hosts/test_parse_nflx_loop_s3n_hdfs.py
# and testdir_multi_jvm/test_benchmark_import.py
# might be able to use more widely
def columnInfoFromInspect(key, exceptionOnMissingValues=True, **kwargs):
    inspect = runInspect(key=key, **kwargs)

    numRows = inspect['numRows']
    numCols = inspect['numCols']
    keyNA = 'naCnt'
    cols = inspect['cols']
    # type
    # key
    # row_size
    # value_size_bytes
    # cols
    # rows
    missingValuesDict = {}
    constantValuesDict = {}
    enumSizeDict = {}
    colNameDict = {}
    colTypeDict = {}
    # all dictionaries created are keyed by col index
    print "Column Summary:"
    for k,c in enumerate(cols):
        # offset
        # base
        # scale
        # mean
        # variance
        # enum_domain_size
        colNameDict[k] = c['name']
        colTypeDict[k] = c['type']
        msg = "%s %d" % (c['name'], k)
        msg += " type: %s" % c['type']
        printMsg = False

        if c['type'] == 'Enum':
            # enums now have 'NaN' returned for min/max

            # if isinstance(c['min'], basestring) or isinstance(c['max'], basestring):
            #     raise Exception("Didn't expect 'min': %s or 'max': %s to be str or unicode" % (c['min'], c['max']))
            cardinality = c['cardinality']
            msg += (" cardinality: %d" % cardinality)
            # inspect2 doesn't have cardinality but this is equivalent
            enumSizeDict[k] = cardinality
            printMsg = True


        if c[keyNA] != 0:
            pct = ((c[keyNA] + 0.0)/ numRows) * 100
            msg += (" %s: %s (%0.1f%s)" % (keyNA, c[keyNA], pct, '%'))
            missingValuesDict[k] = c[keyNA]
            printMsg = True

        if c['min']==c['max'] and (c['type']!='Enum' and c['type']!='enum'):
            msg += (" constant value (min=max): %s" % c['min'])
            constantValuesDict[k] = c['min']
            printMsg = True

        # if the naCnt = numRows, that means it's likely forced NAs..so detect that
        if c[keyNA]==numRows:
            msg += (" constant value (na count = num rows): %s" % c['min'])
            constantValuesDict[k] = c['min']
            printMsg = True

        if printMsg: # don't print ints or floats if ok
            print msg

    if missingValuesDict:
        m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
        print len(missingValuesDict), "columns with missing values", ", ".join(m)
        ### raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

    if constantValuesDict:
        m = [str(k) + ":" + str(v) for k,v in constantValuesDict.iteritems()]
        print len(constantValuesDict), "columns with constant values", ", ".join(m)

    print "\n" + key, \
        "    numRows:", "{:,}".format(numRows), \
        "    numCols:", "{:,}".format(numCols)

    if missingValuesDict and exceptionOnMissingValues:
        m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
        raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

    if numCols != len(colNameDict): 
        raise Exception("numCols: %s doesn't agree with len(colNameDict): %s" % (numCols, len(colNameDict)))

    return (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) 

def infoFromInspect(inspect, csvPathname='none'):
    if not inspect:
        raise Exception("inspect is empty for infoFromInspect")

    # need more info about this dataset for debug
    cols = inspect['cols']
    # look for nonzero num_missing_values count in each col
    keyNA = 'naCnt'
    missingValuesList = []
    for i, colDict in enumerate(cols):
        num_missing_values = colDict[keyNA]
        if num_missing_values != 0:
            print "%s: col: %d, %s: %d" % (csvPathname, i, keyNA, num_missing_values)
            missingValuesList.append(num_missing_values)

    # no type per col in inspect2
    numCols = inspect['numCols']
    numRows = inspect['numRows']
    byteSize = inspect['byteSize']

    print "\n" + csvPathname, "numCols: %s, numRows: %s, byteSize: %s" % \
           (numCols, numRows, byteSize)

    return missingValuesList

# summary doesn't have the # of rows
# we need it to see if na count = number of rows. min/max/mean/sigma/zeros then are ignored (undefined?)
# while we're at it, let's cross check numCols
# if we don't pass these extra params, just ignore
def infoFromSummary(summaryResult, noPrint=False, numCols=None, numRows=None):
    if not summaryResult:
        raise Exception("summaryResult is empty for infoFromSummary")

    summaries = summaryResult['summaries']
    # what if we didn't get the full # of cols in this summary view? 
    # I guess the test should deal with that
    if 1==0 and numCols and (len(summaries)!=numCols):
        raise Exception("Expected numCols: %s cols in summary. Got %s" % (numCols, len(summaries)))

    coltypeList = []
    for column in summaries:
        colname = column['colname']
        # is this always None? unused?
        coltype = column['type']
        nacnt = column['nacnt']
        stats = column['stats']
        stattype = stats['type']
        coltypeList.append(stattype)
        h2o_exec.checkForBadFP(nacnt, 'nacnt for colname: %s stattype: %s' % (colname, stattype))

        if stattype == 'Enum':
            cardinality = stats['cardinality']
            h2o_exec.checkForBadFP(cardinality, 'cardinality for colname: %s stattype: %s' % (colname, stattype))
            
        else:
            mean = stats['mean']
            sd = stats['sd']
            zeros = stats['zeros']
            mins = stats['mins']
            maxs = stats['maxs']
            pct = stats['pct']
            pctile = stats['pctile']

            # check for NaN/Infinity in some of these
            # apparently we can get NaN in the mean for a numerica col with all NA?
            h2o_exec.checkForBadFP(mean, 'mean for colname: %s stattype: %s' % (colname, stattype), nanOkay=True, infOkay=True)
            h2o_exec.checkForBadFP(sd, 'sd for colname: %s stattype %s' % (colname, stattype), nanOkay=True, infOkay=True)
            h2o_exec.checkForBadFP(zeros, 'zeros for colname: %s stattype %s' % (colname, stattype))

            if numRows and (nacnt==numRows):
                print "%s is all NAs with type: %s. no checking for min/max/mean/sigma" % (colname, stattype)
            else:
                if not mins:
                    print dump_json(column)
                    # raise Exception ("Why is min[] empty for a %s col (%s) ? %s %s %s" % (mins, stattype, colname, nacnt, numRows))
                    print "Why is min[] empty for a %s col (%s) ? %s %s %s" % (mins, stattype, colname, nacnt, numRows)
                if not maxs:
                    # this is failing on maprfs best buy...why? (va only?)
                    print dump_json(column)
                    # raise Exception ("Why is max[] empty for a %s col? (%s) ? %s %s %s" % (maxs, stattype, colname, nacnt, numRows))
                    print "Why is max[] empty for a %s col? (%s) ? %s %s %s" % (maxs, stattype, colname, nacnt, numRows)

        hstart = column['hstart']
        hstep = column['hstep']
        hbrk = column['hbrk']
        hcnt = column['hcnt']

        if not noPrint:
            print "\n\n************************"
            print "colname:", colname
            print "coltype:", coltype
            print "nacnt:", nacnt

            print "stattype:", stattype
            if stattype == 'Enum':
                print "cardinality:", cardinality
            else:
                print "mean:", mean
                print "sd:", sd
                print "zeros:", zeros
                print "mins:", mins
                print "maxs:", maxs
                print "pct:", pct
                print "pctile:", pctile

            # histogram stuff
            print "hstart:", hstart
            print "hstep:", hstep
            print "hbrk:", hbrk
            print "hcnt:", hcnt

    return coltypeList

def dot():
    sys.stdout.write('.')
    sys.stdout.flush()

def sleep_with_dot(sec, message=None):
    if message:
        print message
    count = 0
    while count < sec:
        time.sleep(1)
        dot()
        count += 1

def createTestTrain(srcKey, trainDstKey, testDstKey, trainPercent, 
    outputClass=None, outputCol=None, changeToBinomial=False):
    # will have to live with random extract. will create variance

    print "train: get random", trainPercent
    print "test: get remaining", 100 - trainPercent
    if changeToBinomial:
        print "change class", outputClass, "to 1, everything else to 0. factor() to turn real to int (for rf)"

    boundary = (trainPercent + 0.0)/100

    execExpr = ""
    execExpr += "cct.hex=runif(%s,-1);" % srcKey
    execExpr += "%s=%s[cct.hex<=%s,];" % (trainDstKey, srcKey, boundary)
    if changeToBinomial:
        execExpr += "%s[,%s]=%s[,%s]==%s;" % (trainDstKey, outputCol+1, trainDstKey, outputCol+1, outputClass)
        execExpr +=  "factor(%s[, %s]);" % (trainDstKey, outputCol+1)

    h2o_exec.exec_expr(None, execExpr, resultKey=trainDstKey, timeoutSecs=30)

    inspect = runInspect(key=trainDstKey)
    infoFromInspect(inspect, "%s after mungeDataset on %s" % (trainDstKey, srcKey) )

    print "test: same, but use the same runif() random result, complement comparison"

    execExpr = ""
    execExpr += "%s=%s[cct.hex>%s,];" % (testDstKey, srcKey, boundary)
    if changeToBinomial:
        execExpr += "%s[,%s]=%s[,%s]==%s;" % (testDstKey, outputCol+1, testDstKey, outputCol+1, outputClass)
        execExpr +=  "factor(%s[, %s])" % (testDstKey, outputCol+1)
    h2o_exec.exec_expr(None, execExpr, resultKey=testDstKey, timeoutSecs=30)

    inspect = runInspect(key=testDstKey)
    infoFromInspect(inspect, "%s after mungeDataset on %s" % (testDstKey, srcKey) )



# figure out what cols to ignore (opposite of cols+response)
def createIgnoredCols(key, cols, response):
    inspect = runInspect(key=key)
    numCols = inspect['numCols']
    ignore = filter(lambda x:(x not in cols and x!=response), range(numCols))
    ignored_cols = ','.join(map(str,ignore))
    return ignored_cols


# example:
# h2o_cmd.runScore(dataKey=scoreDataKey, modelKey=modelKey, vactual=y, vpredict=1, expectedAuc=0.5)
def runScore(node=None, dataKey=None, modelKey=None, predictKey='Predict.hex', 
    vactual='C1', vpredict=1, expectedAuc=None, expectedAucTol=0.15, doAUC=True, timeoutSecs=200):
    # Score *******************************
    # this messes up if you use case_mode/case_vale above
    predictKey = 'Predict.hex'
    start = time.time()

    predictResult = runPredict(
        data_key=dataKey,
        model_key=modelKey,
        destination_key=predictKey,
        timeoutSecs=timeoutSecs)

    # inspect = runInspect(key=dataKey)
    # print dataKey, dump_json(inspect)

    # just get a predict and AUC on the same data. has to be binomial result
    if doAUC:
        resultAUC = h2o_nodes.nodes[0].generate_auc(
            thresholds=None,
            actual=dataKey,
            predict='Predict.hex',
            vactual=vactual,
            vpredict=vpredict)

        auc = resultAUC['aucdata']['AUC']

        if expectedAuc:
            h2o_util.assertApproxEqual(auc, expectedAuc, tol=expectedAucTol,
                msg="actual auc: %s not close enough to %s" % (auc, expectedAuc))

    # don't do this unless binomial
    predictCMResult = h2o_nodes.nodes[0].predict_confusion_matrix(
        actual=dataKey,
        predict=predictKey,
        vactual=vactual,
        vpredict='predict',
        )

    # print "cm", dump_json(predictCMResult)

    # These will move into the h2o_gbm.py
    # if doAUC=False, means we're not binomial, and the cm is not what we expect
    if doAUC:
        cm = predictCMResult['cm']
        pctWrong = h2o_gbm.pp_cm_summary(cm);
        print h2o_gbm.pp_cm(cm)

    return predictCMResult
        



