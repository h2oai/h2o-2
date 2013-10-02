import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1,java_heap_GB=8)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1,java_heap_GB=8)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_mnist(self):
        importFolderPath = "mnist"
        csvFilename = "train.csv.gz"
        timeoutSecs=1800
        trialStart = time.time()

        # PARSE train****************************************
        trainKey = csvFilename + "_" + ".hex"
        start = time.time()
        parseResult = h2i.import_parse(bucket='smalldata', path=importFolderPath + "/" + csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************

        colsString = ""
        for y in range(0,10):
            if y > 0:
                colsString = colsString + ","
            colsString = colsString + str(y)

        params = { 
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':10,
            'max_depth':8,
            'min_rows':1,
            'cols':colsString,
            'response':784
            }

        kwargs = params.copy()
        h2o.beta_features = True
        timeoutSecs = 1800
        #noPoll -> False when GBM finished
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,**kwargs)
        # hack!
        if h2o.beta_features:
            h2o_jobs.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=120, retryDelaySecs=5)

        print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
            "%f pct. of timeout" % (GBMResult['python_%timeout'])


if __name__ == '__main__':
    h2o.unit_main()
