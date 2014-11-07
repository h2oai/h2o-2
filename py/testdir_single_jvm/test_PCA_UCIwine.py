import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_pca, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_PCA_UCIwine(self):
        csvFilename = "wine.data"
        timeoutSecs=300
        trialStart = time.time()
        #parse
        trainKey = "wine.hex"
        start = time.time()
        parseResult = h2i.import_parse(bucket='smalldata', path=csvFilename, schema='local',
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        #PCA params
        params = { 
            'destination_key': "python_PCA_key",
            'tolerance':0.0,
            'standardize':1
            }   

        kwargs = params.copy()
        #TODO(spencer): Hack around no polling FVEC
        PCAResult = {'python_elapsed': 0, 'python_%timeout': 0}
        start = time.time()
        h2o_cmd.runPCA(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=True, returnFast=False, **kwargs)
        h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=120, retryDelaySecs=2)
        #time.sleep(100)
        elapsed = time.time() - start
        PCAResult['python_elapsed']  = elapsed
        PCAResult['python_%timeout'] = 1.0*elapsed / timeoutSecs
        print "PCA completed in",     PCAResult['python_elapsed'], "seconds.", \
              "%f pct. of timeout" % (PCAResult['python_%timeout'])
        #check PCA results
        pcaView = h2o_cmd.runPCAView(modelKey = "python_PCA_key")
        h2o_pca.simpleCheckPCA(self,pcaView)
        h2o_pca.resultsCheckPCA(self,pcaView)


if __name__ == '__main__':
    h2o.unit_main()
