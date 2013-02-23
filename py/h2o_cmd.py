import os, json, unittest, time, shutil, sys
import h2o
import h2o_browse as h2b, h2o_rf as h2f

def parseFile(node=None, csvPathname=None, key=None, key2=None, 
    timeoutSecs=20, retryDelaySecs=0.5, noise=None, header=None):
    if not csvPathname: raise Exception('No file name specified')
    if not node: node = h2o.nodes[0]
    key = node.put_file(csvPathname, key=key, timeoutSecs=timeoutSecs)
    if key2 is None:
        # don't rely on h2o default key name
        myKey2 = key + '.hex'
    else:
        myKey2 = key2
    return node.parse(key, myKey2, header=header,
        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, noise=noise)

def runInspect(node=None, key=None, timeoutSecs=5, **kwargs):
    if not key: raise Exception('No key for Inspect specified')
    if not node: node = h2o.nodes[0]
    # FIX! currently there is no such thing as a timeout on node.inspect
    return node.inspect(key, **kwargs)

# Not working in H2O yet, but support the test
def runStore2HDFS(node=None, key=None, timeoutSecs=5, **kwargs):
    if not key: raise Exception('No key for Inspect specified')
    if not node: node = h2o.nodes[0]
    # FIX! currently there is no such thing as a timeout on node.inspect
    return node.Store2HDFS(key, **kwargs)

# since we'll be doing lots of execs on a parsed file, not useful to have parse+exec
# retryDelaySecs isn't used, 
def runExecOnly(node=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    return node.exec_query(timeoutSecs, **kwargs)

def runKMeans(node=None, csvPathname=None, key=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    # use 1/5th the KMeans timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,10)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    kmeans = runKMeansOnly(node, parseKey, timeoutSecs, retryDelaySecs, **kwargs)
    return kmeans

def runKMeansOnly(node=None, parseKey=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseKey: raise Exception('No parsed key for KMeans specified')
    if not node: node = h2o.nodes[0]
    print parseKey['destination_key']
    return node.kmeans(parseKey['destination_key'], None, timeoutSecs, retryDelaySecs, **kwargs)

def runGLM(node=None, csvPathname=None, key=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    # use 1/5th the GLM timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,10)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    glm = runGLMOnly(node, parseKey, timeoutSecs, retryDelaySecs, **kwargs)
    return glm

def runGLMOnly(node=None, parseKey=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseKey: raise Exception('No parsed key for GLM specified')
    if not node: node = h2o.nodes[0]
    # no such thing as GLMView..don't use retryDelaySecs
    return node.GLM(parseKey['destination_key'], timeoutSecs, **kwargs)

# FIX! how do we run RF score on another model?
def runGLMScore(node=None, key=None, model_key=None, timeoutSecs=20):
    if not node: node = h2o.nodes[0]
    return node.GLMScore(key, model_key, timeoutSecs)

def runGLMGrid(node=None, csvPathname=None, key=None, 
        timeoutSecs=60, retryDelaySecs=2, **kwargs):
    # use 1/5th the GLM timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,10)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    glm = runGLMGridOnly(node, parseKey, timeoutSecs, retryDelaySecs, **kwargs)
    return glm

def runGLMGridOnly(node=None,parseKey=None,
        timeoutSecs=20,retryDelaySecs=2,**kwargs):
    if not parseKey: raise Exception('No parsed key for GLMGrid specified')
    if not node: node = h2o.nodes[0]
    # no such thing as GLMGridView..don't use retryDelaySecs
    return node.GLMGrid(parseKey['destination_key'], timeoutSecs, **kwargs)

def runRF(node=None, csvPathname=None, trees=5, key=None, 
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    # use 1/5th the RF timeoutSecs for allowed parse time.
    pto = max(timeoutSecs/5,30)
    noise = kwargs.pop('noise',None)
    parseKey = parseFile(node, csvPathname, key, timeoutSecs=pto, noise=noise)
    return runRFOnly(node, parseKey, trees, timeoutSecs, retryDelaySecs, **kwargs)

def runRFTreeView(node=None, n=None, data_key=None, model_key=None, timeoutSecs=20, **kwargs):
    if not node: node = h2o.nodes[0]
    return node.random_forest_treeview(n, data_key, model_key, timeoutSecs, **kwargs)

def runRFOnly(node=None, parseKey=None, trees=5,
        timeoutSecs=20, retryDelaySecs=2, **kwargs):
    if not parseKey: raise Exception('No parsed key for RF specified')
    if not node: node = h2o.nodes[0]
    #! FIX! what else is in parseKey that we should check?
    h2o.verboseprint("runRFOnly parseKey:", parseKey)
    Key = parseKey['destination_key']
    rf = node.random_forest(Key, trees, timeoutSecs, **kwargs)

    # FIX! check all of these somehow?
    # if we model_key was given to rf via **kwargs, remove it, since we're passing 
    # model_key from rf. can't pass it in two places. (ok if it doesn't exist in kwargs)
    data_key  = rf['data_key']
    kwargs.pop('model_key',None)
    kwargs.pop('model_key',None)
    model_key = rf['model_key']
    rfCloud = rf['response']['h2o']

    # same thing. if we use random param generation and have ntree in kwargs, get rid of it.
    kwargs.pop('ntree',None)

    # this is important. it's the only accurate value for how many trees RF was asked for.
    ntree    = rf['ntree']

    # /ip:port of cloud (can't use h2o name)
    rfClass= rf['response_variable']

    rfView = runRFView(node, data_key, model_key, ntree, timeoutSecs, retryDelaySecs, **kwargs)
    return rfView

# scoring on browser does these:
# RFView.html?
# data_key=a5m.hex&
# model_key=__RFModel_81c5063c-e724-4ebe-bfc1-3ac6838bc628&
# FIX! why not in model?
# response_variable=1&
# FIX why needed?
# ntree=50&
# FIX! why not in model
# class_weights=-1%3D1.0%2C0%3D1.0%2C1%3D1.0&
# FIX! no longer?
# out_of_bag_error_estimate=1&
# FIX! scoring only
# no_confusion_matrix=1&
# clear_confusion_matrix=1

def runRFView(node=None, data_key=None, model_key=None, ntree=None, timeoutSecs=15, retryDelaySecs=2, **kwargs):
    if not node: node = h2o.nodes[0]
    def test(n, tries=None):
        rfView = n.random_forest_view(data_key, model_key, timeoutSecs, **kwargs)
        status = rfView['response']['status']
        numberBuilt = rfView['trees']['number_built']

        if status == 'done': 
            if numberBuilt!=ntree: 
                raise Exception("RFview done but number_built!=ntree: %s %s", 
                    numberBuilt, ntree)
            return True
        if status != 'poll': raise Exception('Unexpected status: ' + status)

        progress = rfView['response']['progress']
        progressTotal = rfView['response']['progress_total']

        # want to double check all this because it's new
        # and we had problems with races/doneness before
        errorInResponse = \
            numberBuilt<0 or ntree<0 or numberBuilt>ntree or \
            progress<0 or progressTotal<0 or progress>progressTotal or \
            progressTotal!=(ntree+1) or \
            ntree!=rfView['ntree']
            # rfView better always agree with what RF ntree was

        if errorInResponse:
            raise Exception("\nBad values in response during RFView polling.\n" + 
                "progress: %s, progressTotal: %s, ntree: %s, numberBuilt: %s, status: %s" % \
                (progress, progressTotal, ntree, numberBuilt, status))

        # don't print the useless first poll.
        # UPDATE: don't look for done. look for not poll was missing completion when looking for done
        if (status=='poll'):
            if numberBuilt==0:
                h2o.verboseprint(".")
            else:
                h2o.verboseprint("\nRFView polling #", tries,
                    "Status: %s. %s trees done of %s desired" % (status, numberBuilt, ntree))

        return (status!='poll')

    node.stabilize(
            test,
            'random forest reporting %d trees' % ntree,
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

    # kind of wasteful re-read, but maybe good for testing
    rfView = node.random_forest_view(data_key, model_key, timeoutSecs, **kwargs)
    h2f.simpleCheckRFView(node, rfView)

    return rfView
