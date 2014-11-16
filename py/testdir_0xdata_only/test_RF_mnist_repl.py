import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf
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

    def test_RF_mnist_both(self):
        csvFilelist = [
            # ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, 784834182943470027),
            ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, None, '*mnist_training*gz'),
            ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, None, '*mnist_training*gz'),
            ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, None, '*mnist_training*gz'),
            ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, None, '*mnist_training*gz'),
        ]
        # IMPORT**********************************************

        trial = 0
        allDelta = []
        importFolderPath = "mnist"
        for (trainCsvFilename, testCsvFilename, timeoutSecs, rfSeed, parsePattern) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey = testCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            csvPathname = importFolderPath + "/" + testCsvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            print "We won't use this pruning of x on test data. See if it prunes the same as the training"
            y = 0 # first column is pixel value
            print "y:"
            x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)

            # PARSE train****************************************
            print "Use multi-file parse to grab both the mnist_testing.csv.gz and mnist_training.csv.gz for training"
            trainKey = trainCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            csvPathname = importFolderPath + "/" + parsePattern
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=parsePattern, schema='local', timeoutSecs=300)
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # RF+RFView (train)****************************************
            print "This is the 'ignore=' we'll use"
            y = 0 # first column is pixel value
            ignore_x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300, returnIgnoreX=True)
            ntree = 100
            params = {
                'response': y,
                'ignored_cols': ignore_x, 
                'ntrees': ntree,
                # 'data_key='mnist_training.csv.hex'
                'mtries': 28, # fix because we ignore some cols, which will change the srt(cols) calc?
                'max_depth': 500,
                'destination_key': 'RF_model',
                'nbins': 1024,
                }

            if rfSeed is None:
                params['seed'] = random.randint(0,sys.maxint)
            else:
                params['seed'] = rfSeed
            print "RF seed:", params['seed']

            kwargs = params.copy()
            print "Trying rf"
            timeoutSecs = 1800
            start = time.time()
            rfView = h2o_cmd.runRF(parseResult=parseResult, 
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, retryDelaySecs=2, **kwargs)
            elapsed = time.time() - start
            print "RF completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_rf.simpleCheckRFView(None, rfView, **params)
            modelKey = rfView['model_key']

            # RFView (score on test)****************************************
            start = time.time()
            kwargs = {'response': y}
            rfView = h2o_cmd.runRFView(data_key=testKey, model_key=modelKey, 
                timeoutSecs=60, pollTimeoutSecs=60, noSimpleCheck=False, **kwargs)
            elapsed = time.time() - start
            print "RFView in",  elapsed, "secs", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(None, rfView, **params)
            print "classification error is expected to be low because we included the test data in with the training!"
            self.assertAlmostEqual(classification_error, 0.028, delta=0.01, msg="Classification error %s differs too much" % classification_error)
        
            treeStats = rfView['drf_model']['treesStats']
            # Expected values are from this case:
            # ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, 784834182943470027),
            expected =  {'minLeaves': 4996, 'meanLeaves': 5064.1, 'maxLeaves': 5148}
            expected += {'minDepth': 21, 'meanDepth': 23.8, 'maxDepth': 25}
            for key in expected:
                delta = ((expected[key]- actual[key])/expected[key]) * 100
                d = "seed: %s %s %s %s %s pct. different %s" % (params['seed'], key, actual[key], expected[key], delta)
                print d
                allDelta.append(d)
                # FIX! should change this to an assert?

            # Predict (on test)****************************************
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=modelKey, data_key=testKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "generate_predictions in",  elapsed, "secs", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

        # Done *******************************************************
        print "\nShowing the results again from all the trials, to see variance"
        for d in allDelta:
            print d

if __name__ == '__main__':
    h2o.unit_main()
