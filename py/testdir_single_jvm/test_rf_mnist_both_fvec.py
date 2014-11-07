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

    def test_rf_mnist_both_fvec(self):
        importFolderPath = "mnist"
        csvFilelist = [
            # ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, 784834182943470027),
            ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, None, '*mnist*gz'),
            # to see results a 2nd time
            ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, None, '*mnist*gz'),
        ]
        # IMPORT**********************************************
        # since H2O deletes the source key, we should re-import every iteration if we re-use the src in the list
        (importFolderResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=importFolderPath + "/*")
        ### print "importHDFSResult:", h2o.dump_json(importFolderResult)
        if 'files' in importFolderResult:
            succeededList = importFolderResult['files']
        else:
            succeededList = importFolderResult['succeeded']

        ### print "succeededList:", h2o.dump_json(succeededList)

        self.assertGreater(len(succeededList),1,"Should see more than 1 files in the import?")
        # why does this hang? can't look at storeview after import?
        print "\nTrying StoreView after the import folder"
        h2o_cmd.runStoreView(timeoutSecs=30)

        trial = 0
        allDelta = []
        for (trainCsvFilename, testCsvFilename, timeoutSecs, rfSeed, parsePattern) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey2 = testCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath+"/"+testCsvFilename,
                hex_key=testKey2, timeoutSecs=timeoutSecs)
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
            trainKey2 = trainCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath+"/"+parsePattern,
                hex_key=trainKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # RF+RFView (train)****************************************
            print "Not using ignore from this..have to adjust cols?"
            h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300, returnIgnoreX=True)
            ntree = 2
            params = {
                'response': 'C1',
                # 'ignored_cols_by_name': ignore_x, 
                'ntrees': ntree,
                'mtries': 28, # fix because we ignore some cols, which will change the srt(cols) calc?
                'max_depth': 20,
                'sample_rate': 0.67,
                'destination_key': 'RF_model',
                'nbins': 100,
                'importance': 0,
                'balance_classes': 0,
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
            rfView = h2o_cmd.runRF(parseResult=parseResult, rfView=True,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, retryDelaySecs=2, **kwargs)
            elapsed = time.time() - start
            print "RF completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            # print 'rfView:', h2o.dump_json(rfView)
            h2o_rf.simpleCheckRFView(None, rfView, **params)
            modelKey = rfView['drf_model']['_key']

            # RFView (score on test)****************************************
            start = time.time()
            # FIX! 1 on oobe causes stack trace?
            kwargs = {'response': y}
            rfView = h2o_cmd.runRFView(data_key=testKey2, model_key=modelKey, ntree=ntree, out_of_bag_error_estimate=0, 
                timeoutSecs=60, pollTimeoutSecs=60, noSimpleCheck=False, **kwargs)
            elapsed = time.time() - start
            print "RFView in",  elapsed, "secs", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(None, rfView, **params)
            # training and test data are unique, so error won't be low?
            # self.assertAlmostEqual(classification_error, 0.0003, delta=0.0003, msg="Classification error %s differs too much" % classification_error)

            leaves = {
                'min': rfView['drf_model']['treeStats']['minLeaves'],
                'mean': rfView['drf_model']['treeStats']['meanLeaves'],
                'max': rfView['drf_model']['treeStats']['maxLeaves'],
            }
            # Expected values are from this case:
            # ("mnist_training.csv.gz", "mnist_testing.csv.gz", 600, 784834182943470027),
            leavesExpected = {'min': 537, 'mean': 1118.05, 'max': 1701}
            for l in leaves:
                # self.assertAlmostEqual(leaves[l], leavesExpected[l], delta=10, msg="leaves %s %s %s differs too much" % (l, leaves[l], leavesExpected[l]))
                delta = ((leaves[l] - leavesExpected[l])/leaves[l]) * 100
                d = "seed: %s leaves %s %s %s pct. different %s" % (params['seed'], l, leaves[l], leavesExpected[l], delta)
                print d
                allDelta.append(d)

            depth = {
                'min': rfView['drf_model']['treeStats']['minDepth'],
                'mean': rfView['drf_model']['treeStats']['meanDepth'],
                'max': rfView['drf_model']['treeStats']['maxDepth'],
            }
            depthExpected = {'min': 20, 'mean': 20, 'max': 20}
            for l in depth:
                # self.assertAlmostEqual(depth[l], depthExpected[l], delta=1, msg="depth %s %s %s differs too much" % (l, depth[l], depthExpected[l]))
                delta = ((depth[l] - depthExpected[l])/leaves[l]) * 100
                d = "seed: %s depth %s %s %s pct. different %s" % (params['seed'], l, depth[l], depthExpected[l], delta)
                print d
                allDelta.append(d)

            # Predict (on test)****************************************
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=modelKey, data_key=testKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "generate_predictions in",  elapsed, "secs", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

        # Done *******************************************************
        print "\nShowing the results again from all the trials, to see variance"
    
        for d in allDelta:
            print d

if __name__ == '__main__':
    h2o.unit_main()
