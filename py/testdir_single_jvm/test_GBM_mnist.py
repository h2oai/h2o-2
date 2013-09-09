import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i, h2o_glm, h2o_util, h2o_rf
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(1, java_heap_GB=8)

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
        params = { 
            'destination_key': "GBMKEY",
            'learn_rate':.1,
            'ntrees':10,
            'max_depth':8,
            'min_rows':1,
            'vresponse':784
            }

        kwargs = params.copy()
        h2o.beta_features = True
        timeoutSecs = 1800
        start = time.time()
        node = h2o.nodes[0]
        GBMResult = node.gbm(data_key=trainKey, **kwargs)
        elapsed = time.time() - start
        print "GBM completed in", elapsed, "seconds.", \
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
