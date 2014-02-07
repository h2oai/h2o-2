import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=14)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_mnist_both(self):
        csvFilelist = [
            # ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, 784834182943470027),
            ("mnist_training.csv.gz", "mnist_testing_0.csv.gz", 600, None, '*mnist_training*gz'),
            ("mnist_training.csv.gz", "mnist_testing_0.csv.gz", 600, None, '*mnist_training*gz'),
            ("mnist_training.csv.gz", "mnist_testing_0.csv.gz", 600, None, '*mnist_training*gz'),
            ("mnist_training.csv.gz", "mnist_testing_0.csv.gz", 600, None, '*mnist_training*gz'),
        ]
        # IMPORT**********************************************

        trial = 0
        allDelta = []
        importFolderPath = "mnist_repl"
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
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=parsePattern, schema='local',
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # RF+RFView (train)****************************************
            print "This is the 'ignore=' we'll use"
            ignore_x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300, forRF=True)
            ntree = 100
            params = {
                'response_variable': 0,
                'ignore': ignore_x, 
                'ntree': ntree,
                'iterative_cm': 1,
                'out_of_bag_error_estimate': 1,
                # 'data_key='mnist_training.csv.hex'
                'features': 28, # fix because we ignore some cols, which will change the srt(cols) calc?
                'exclusive_split_limit': None,
                'depth': 2147483647,
                'stat_type': 'ENTROPY',
                'sampling_strategy': 'RANDOM',
                'sample': 67,
                # 'model_key': '__RFModel_7055e6cf-a0de-44db-b165-f5994730ac77',
                'model_key': 'RF_model',
                'bin_limit': 1024,
                # 'seed': 784834182943470027,
                'use_non_local_data': 0,
                'class_weights': '0=1.0,1=1.0,2=1.0,3=1.0,4=1.0,5=1.0,6=1.0,7=1.0,8=1.0,9=1.0',
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
            rfView = h2o_cmd.runRF(parseResult=parseResult, rfView=False,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, retryDelaySecs=2, **kwargs)
            elapsed = time.time() - start
            print "RF completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_rf.simpleCheckRFView(None, rfView, **params)
            modelKey = rfView['model_key']

            # RFView (score on test)****************************************
            start = time.time()
            # FIX! 1 on oobe causes stack trace?
            kwargs = {'response_variable': y}
            rfView = h2o_cmd.runRFView(data_key=testKey, model_key=modelKey, ntree=ntree, out_of_bag_error_estimate=0, 
                timeoutSecs=60, pollTimeoutSecs=60, noSimpleCheck=False, **kwargs)
            elapsed = time.time() - start
            print "RFView in",  elapsed, "secs", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(None, rfView, **params)
            print "classification error is expected to be low because we included the test data in with the training!"
            self.assertAlmostEqual(classification_error, 0.028, delta=0.01, msg="Classification error %s differs too much" % classification_error)
        
            leaves = rfView['trees']['leaves']
            # Expected values are from this case:
            # ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, 784834182943470027),
            leavesExpected = {'min': 4996, 'mean': 5064.1, 'max': 5148}
            for l in leaves:
                # self.assertAlmostEqual(leaves[l], leavesExpected[l], delta=10, msg="leaves %s %s %s differs too much" % (l, leaves[l], leavesExpected[l]))
                delta = ((leaves[l] - leavesExpected[l])/leaves[l]) * 100
                d = "seed: %s leaves %s %s %s pct. different %s" % (params['seed'], l, leaves[l], leavesExpected[l], delta)
                print d
                allDelta.append(d)

            depth = rfView['trees']['depth']
            depthExpected = {'min': 21, 'mean': 23.8, 'max': 25}
            for l in depth:
                # self.assertAlmostEqual(depth[l], depthExpected[l], delta=1, msg="depth %s %s %s differs too much" % (l, depth[l], depthExpected[l]))
                delta = ((depth[l] - depthExpected[l])/leaves[l]) * 100
                d = "seed: %s depth %s %s %s pct. different %s" % (params['seed'], l, depth[l], depthExpected[l], delta)
                print d
                allDelta.append(d)

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
