import os, json, unittest, time, shutil, sys, socket
import h2o
import h2o_browse as h2b, h2o_rf as h2f

def parseS3File(node=None, bucket=None, filename=None, keyForParseResult=None, 
    timeoutSecs=20, retryDelaySecs=2, pollTimeoutSecs=30, 
    noise=None, noPoll=None, **kwargs):
    ''' Parse a file stored in S3 bucket'''                                                                                                                                                                       
    if not bucket  : raise Exception('No S3 bucket')
    if not filename: raise Exception('No filename in bucket')
    if not node: node = h2o.nodes[0]
    
    import_result = node.import_s3(bucket)
    s3_key = [f['key'] for f in import_result['succeeded'] if f['file'] == filename ][0]
    
    if keyForParseResult is None:
        myKeyForParseResult = s3_key + '.hex'
    else:
        myKeyForParseResult = keyForParseResult
    p = node.parse(s3_key, myKeyForParseResult, 
        timeoutSecs, retryDelaySecs, 
        pollTimeoutSecs=pollTimeoutSecs, noise=noise, noPoll=noPoll, **kwargs)

    # do SummaryPage here too, just to get some coverage
    node.summary_page(myKeyForParseResult)
    return p

def runInspect(node=None, key=None, timeoutSecs=5, **kwargs):
    if not key: raise Exception('No key for Inspect')
    if not node: node = h2o.nodes[0]
    return node.inspect(key, timeoutSecs=timeoutSecs, **kwargs)

def runSummary(node=None, key=None, timeoutSecs=30, **kwargs):
    if not key: raise Exception('No key for Summary')
    if not node: node = h2o.nodes[0]
    return node.summary_page(key, timeoutSecs=timeoutSecs, **kwargs)

# Not working in H2O yet, but support the test
def runStore2HDFS(node=None, key=None, timeoutSecs=5, **kwargs):
    if not key: raise Exception('No key for Inspect')
    if not node: node = h2o.nodes[0]
    # FIX! currently there is no such thing as a timeout on node.inspect
    return node.Store2HDFS(key, **kwargs)

# since we'll be doing lots of execs on a parsed file, not useful to have parse+exec
# retryDelaySecs isn't used, 
def runExec(node=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    return node.exec_query(timeoutSecs, **kwargs)

def runKMeans(node=None, parseResult=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseResult: raise Exception('No parseResult for KMeans')
    if not node: node = h2o.nodes[0]
    print parseResult['destination_key']
    return node.kmeans(parseResult['destination_key'], None, 
        timeoutSecs, retryDelaySecs, **kwargs)

def runKMeansGrid(node=None, parseResult=None,
        timeoutSecs=60, retryDelaySecs=2, noise=None, **kwargs):
    if not parseResult: raise Exception('No parseResult for KMeansGrid')
    if not node: node = h2o.nodes[0]
    # no such thing as KMeansGridView..don't use retryDelaySecs
    return node.kmeans_grid(parseResult['destination_key'], timeoutSecs, **kwargs)

def runGLM(node=None, parseResult=None, 
        timeoutSecs=20, retryDelaySecs=2, noise=None, **kwargs):
    if not parseResult: raise Exception('No parseResult for GLM')
    if not node: node = h2o.nodes[0]
    return node.GLM(parseResult['destination_key'], 
        timeoutSecs, retryDelaySecs, noise=noise, **kwargs)

def runGLMScore(node=None, key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    return node.GLMScore(key, model_key, timeoutSecs, **kwargs)

def runGLMGrid(node=None, parseResult=None,
        timeoutSecs=60, retryDelaySecs=2, noise=None, **kwargs):
    if not parseResult: raise Exception('No parseResult for GLMGrid')
    if not node: node = h2o.nodes[0]
    # no such thing as GLMGridView..don't use retryDelaySecs
    return node.GLMGrid(parseResult['destination_key'], timeoutSecs, **kwargs)

def runPCA(node=None, parseResult=None, timeoutSecs=600, **kwargs):
    if not parseResult: raise Exception('No parseResult for PCA')
    if not node: node = h2o.nodes[0]
    data_key = parseResult['destination_key']
    return node.pca(data_key=data_key, **kwargs)

def runNNet(node=None, parseResult=None, timeoutSecs=600, **kwargs):
    if not parseResult: raise Exception('No parseResult for NN')
    if not node: node = h2o.nodes[0]
    data_key = parseResult['destination_key']
    return node.neural_net(data_key=data_key, **kwargs)

def runGBM(node=None, parseResult=None, timeoutSecs=500, **kwargs):
    if not parseResult: raise Exception('No parseResult for GBM')
    if not node: node = h2o.nodes[0]
    data_key = parseResult['destination_key']
    return node.gbm(data_key=data_key, timeoutSecs=timeoutSecs,**kwargs) 

def runPredict(node=None, data_key=None, model_key=None, timeoutSecs=500, **kwargs):
    if not data_key: raise Exception('No data_key for run Predict')
    if not node: node = h2o.nodes[0]
    return node.generate_predictions(data_key, model_key, timeoutSecs=timeoutSecs,**kwargs) 

# rfView can be used to skip the rf completion view
# for creating multiple rf jobs
def runRF(node=None, parseResult=None, trees=5, 
        timeoutSecs=20, retryDelaySecs=2, rfView=True, noise=None, noPrint=False, **kwargs):
    if not parseResult: raise Exception('No parseResult for RF')
    if not node: node = h2o.nodes[0]
    #! FIX! what else is in parseResult that we should check?
    h2o.verboseprint("runRF parseResult:", parseResult)
    Key = parseResult['destination_key']
    return node.random_forest(Key, trees, timeoutSecs, **kwargs)

def runRFTreeView(node=None, n=None, data_key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    return node.random_forest_treeview(n, data_key, model_key, timeoutSecs, **kwargs)

def runGBMView(node=None,model_key=None,timeoutSecs=300,retryDelaySecs=2,noPoll=False,**kwargs):
    if not node: node = h2o.nodes[0]
    if not model_key: 
        raise Exception("\nNo model_key was supplied to the gbm view!")
    gbmView = node.gbm_view(model_key,timeoutSecs=timeoutSecs)
    return gbmView

def runRFView(node=None, data_key=None, model_key=None, ntree=None, 
    timeoutSecs=15, retryDelaySecs=2, doSimpleCheck=True,
    noise=None, noPoll=False, noPrint=False, **kwargs):
    if not node: node = h2o.nodes[0]

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_view(data_key, model_key, ntree=ntree, timeoutSecs=timeoutSecs, noise=noise, **kwargs)
    if doSimpleCheck:
        h2f.simpleCheckRFView(node, rfView, noPrint=noPrint)
    return rfView

def runRFScore(node=None, data_key=None, model_key=None, ntree=None, 
    timeoutSecs=15, retryDelaySecs=2, doSimpleCheck=True,
    noise=None, noPoll=False, noPrint=False, **kwargs):
    if not node: node = h2o.nodes[0]

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_score(data_key, model_key, timeoutSecs, noise=noise, **kwargs)
    if doSimpleCheck:
        h2f.simpleCheckRFView(node, rfView, noPrint=noPrint)
    return rfView

def runStoreView(node=None, timeoutSecs=30, **kwargs):
    if not node: node = h2o.nodes[0]
    storeView = node.store_view(timeoutSecs, **kwargs)
    for s in storeView['keys']:
        print "StoreView: key:", s['key']
        if 'rows' in s: 
            h2o.verboseprint("StoreView: rows:", s['rows'], "value_size_bytes:", s['value_size_bytes'])
    h2o.verboseprint('storeView has', len(storeView['keys']), 'keys')
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
    h2o.verboseprint("Waiting for {0}:{1} {2}times...".format(ip,port,retries))
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
    c = h2o.nodes[0].get_cloud()
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
    num_rows = inspect['num_rows']
    num_cols = inspect['num_cols']
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
        if c['type'] == 'enum':
            msg += (" enum_domain_size: %d" % c['enum_domain_size'])
            enumSizeDict[k] = c['enum_domain_size']
            printMsg = True

        if c['num_missing_values'] != 0:
            pct = ((c['num_missing_values'] + 0.0)/ num_rows) * 100
            msg += (" num_missing_values: %s (%0.1f%s)" % (c['num_missing_values'], pct, '%'))
            missingValuesDict[k] = c['num_missing_values']
            printMsg = True

        if c['min'] == c['max']:
            msg += (" constant value: %s" % c['min'])
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
        "    num_rows:", "{:,}".format(num_rows), \
        "    num_cols:", "{:,}".format(num_cols)

    if missingValuesDict and exceptionOnMissingValues:
        m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
        raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

    if num_cols != len(colNameDict): 
        raise Exception("num_cols: %s doesn't agree with len(colNameDict): %s" % (num_cols, len(colNameDict)))

    return (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) 

def infoFromInspect(inspect, csvPathname):
    # need more info about this dataset for debug
    cols = inspect['cols']
    # look for nonzero num_missing_values count in each col
    missingValuesList = []
    for i, colDict in enumerate(cols):
        num_missing_values = colDict['num_missing_values']
        if num_missing_values != 0:
            print "%s: col: %d, num_missing_values: %d" % (csvPathname, i, num_missing_values)
            missingValuesList.append(num_missing_values)

    num_cols = inspect['num_cols']
    num_rows = inspect['num_rows']
    row_size = inspect['row_size']
    ptype = inspect['type']
    value_size_bytes = inspect['value_size_bytes']
    response = inspect['response']
    ptime = response['time']

    print "\n" + csvPathname, "num_cols: %s, num_rows: %s, row_size: %s, ptype: %s, value_size_bytes: %s" % \
           (num_cols, num_rows, row_size, ptype, value_size_bytes)
    return missingValuesList

def infoFromSummary(summaryResult, noPrint=False):
    summary = summaryResult['summary']
    columnsList = summary['columns']
    for columns in columnsList:
        N = columns['N']
        # self.assertEqual(N, rowCount)
        name = columns['name']
        stype = columns['type']
        histogram = columns['histogram']
        bin_size = histogram['bin_size']
        bin_names = histogram['bin_names']
        if not noPrint:
            for b in bin_names:
                print "bin_name:", b

        bins = histogram['bins']
        nbins = histogram['bins']
        if not noPrint:
            print "\n\n************************"
            print "N:", N
            print "name:", name
            print "type:", stype
            print "bin_size:", bin_size
            print "len(bin_names):", len(bin_names), bin_names
            print "len(bins):", len(bins), bins
            print "len(nbins):", len(nbins), nbins

        # not done if enum
        if stype != "enum":
            zeros = columns['zeros']
            na = columns['na']
            smax = columns['max']
            smin = columns['min']
            mean = columns['mean']
            sigma = columns['sigma']
            if not noPrint:
                print "zeros:", zeros
                print "na:", na
                print "smax:", smax
                print "smin:", smin
                print "mean:", mean
                print "sigma:", sigma

            # sometimes we don't get percentiles? (if 0 or 1 bins?)
            if len(bins) >= 2:
                percentiles = columns['percentiles']
                thresholds = percentiles['thresholds']
                values = percentiles['values']

                if not noPrint:
                    # h2o shows 5 of them, ordered
                    print "len(max):", len(smax), smax
                    print "len(min):", len(smin), smin
                    print "len(thresholds):", len(thresholds), thresholds
                    print "len(values):", len(values), values

                for v in values:
                    # 0 is the most max or most min
                   if not v >= smin[0]:
                        m = "Percentile value %s should all be >= the min dataset value %s" % (v, smin[0])
                        raise Exception(m)
                   if not v <= smax[0]:
                        m = "Percentile value %s should all be <= the max dataset value %s" % (v, smax[0])
                        raise Exception(m)

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
