import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j

DO_DELETE_KEYS_AND_CAUSE_PROBLEM = False

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(node_count=2,java_heap_GB=7)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_mnist_restart_many(self):
        importFolderPath = "mnist"
        csvFilename = "train.csv.gz"
        timeoutSecs=1800
        trialStart = time.time()

        for trial in range(10):
            # PARSE train****************************************
            trainKey = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path=importFolderPath + "/" + csvFilename, schema='put',
                hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # GBM (train)****************************************
            params = { 
                'destination_key': "GBMKEY",
                'learn_rate': .1,
                'ntrees': 10,
                'max_depth': 8,
                'min_rows': 1,
                'response': 784, # this dataset has the response in the last col (0-9 to check)
                # 'ignored_cols_by_name': range(200,784) # only use the first 200 for speed?
                }

            kwargs = params.copy()
            timeoutSecs = 1800
            #noPoll -> False when GBM finished
            GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True, **kwargs)
            # if it fails, should happen within 8 secs
            time.sleep(8)
            h2j.cancelAllJobs()
            h2o.check_sandbox_for_errors()
            print "Trial %s: GBM start didn't have any errors after 8 seconds. cancelled. Will delete all keys now." % trial

            if DO_DELETE_KEYS_AND_CAUSE_PROBLEM:
                h2i.delete_keys_at_all_nodes()
            # FIX! does the delete really complete fully before we get a response?
            # time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
