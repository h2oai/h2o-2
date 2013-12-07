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
            h2o.build_cloud(node_count=1,java_heap_GB=13) # fails with 13
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_mnist_fvec(self):
        h2o.beta_features = True
        importFolderPath = "mnist"
        csvFilename = "train.csv.gz"
        timeoutSecs=1800
        trialStart = time.time()

        # PARSE train****************************************
        trainKey = csvFilename + "_" + ".hex"
        start = time.time()
        parseResult = h2i.import_parse(bucket='smalldata', path=importFolderPath + "/" + csvFilename, schema='put',
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        params = { 
            'classification': 0, # faster? 
            'destination_key': "GBMKEY",
            'learn_rate': .1,
            'ntrees': 3,
            'max_depth': 8,
            'min_rows': 1,
            'response': 'C784', # this dataset has the response in the last col (0-9 to check)
            # 'ignored_cols_by_name': range(200,784) # only use the first 200 for speed?
            }

        kwargs = params.copy()
        h2o.beta_features = True
        timeoutSecs = 1800
        #noPoll -> False when GBM finished
        start = time.time()
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, **kwargs)
        elapsed = time.time() - start

        print "GBM training completed in", elapsed, "seconds.", \
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)


if __name__ == '__main__':
    h2o.unit_main()
