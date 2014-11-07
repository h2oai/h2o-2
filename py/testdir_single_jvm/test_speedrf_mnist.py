import unittest
import sys
import time
sys.path.extend(['.','..','../..','py'])

import h2o
import h2o_cmd
import h2o_browse as h2b
import h2o_import as h2i
import h2o_glm
import h2o_util
import h2o_rf


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1, java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_speedrf_mnist(self):
        importFolderPath = "mnist"
        csvFilelist = [
            # ("mnist_reals_testing.csv.gz", "mnist_reals_testing.csv.gz",    600), 
            # ("a.csv", "b.csv", 60),
            # ("mnist_reals_testing.csv.gz", "mnist_reals_testing.csv.gz",    600), 
            ("train.csv.gz", "test.csv.gz", 600),
            ]
        trial = 0
        for (trainCsvFilename, testCsvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey2 = testCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path=importFolderPath + "/" + testCsvFilename,
                                           hex_key=testKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds', \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            print "We won't use this pruning of x on test data. See if it prunes the same as the training"
            y = 784 # last column is pixel value
            print "y:"
            x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)

            # PARSE train****************************************
            trainKey2 = trainCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path=importFolderPath + "/" + trainCsvFilename,
                                           hex_key=trainKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds', \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # RF+RFView (train)****************************************
            ignore_x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300, returnIgnoreX=True)
            ntrees = 10
            params = {
                'response': y,
                'ignored_cols_by_name': ignore_x,
                'ntrees': ntrees,
                'mtries': 28, # fix because we ignore some cols, which will change the srt(cols) calc?
                'max_depth': 15,
                'sample_rate': 0.67,
                'destination_key': 'SpeeDRF_model',
                'nbins': 1024,
                'seed': 784834182943470027,
                'oobee': 1,
                }
            kwargs = params.copy()
            print "Trying rf"
            timeoutSecs = 1800
            start = time.time()
            rfv = h2o_cmd.runSpeeDRF(parseResult=parseResult,
                                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, retryDelaySecs=2, **kwargs)
            elapsed = time.time() - start
            print "RF completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            rfv["drf_model"] = rfv.pop("speedrf_model")
            h2o_rf.simpleCheckRFView(None, rfv, **params)
            rf_model = rfv['drf_model']
            used_trees = rf_model['N']
            data_key = rf_model['_dataKey']
            model_key = rf_model['_key']

            print "Total trees: ", used_trees
            print "On data key: ", data_key
            print "Produced model key: ", model_key

if __name__ == '__main__':
    h2o.unit_main()
