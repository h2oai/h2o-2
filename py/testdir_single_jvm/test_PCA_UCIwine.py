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

    def test_PCA_UCIwine(self):
        csvFilename = "wine.data"
        timeoutSecs=180
        trialStart = time.time()

        # PARSE ****************************************
        trainKey = csvFilename + "_" + ".hex"
        start = time.time()
        parseResult = h2i.import_parse(bucket='smalldata', path=csvFilename,
            hex_key=trainKey, timeoutSecs=timeoutSecs)
        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # PCA****************************************
        params = { 
            'destination_key': "python_PCA_key",
            'ignore':0,
            'tolerance':0.0,
            'standardize':1
            }   

        kwargs = params.copy()
        start = time.time()
        node = h2o.nodes[0]
        PCAResult = node.pca(data_key=trainKey, **kwargs)
        elapsed = time.time() - start
        print "PCA completed in", elapsed, "seconds.", \
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

if __name__ == '__main__':
    h2o.unit_main()
