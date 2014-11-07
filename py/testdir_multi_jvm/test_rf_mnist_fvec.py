import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs

DO_POLL = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1, java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_mnist_fvec(self):
        importFolderPath = "mnist"
        csvFilelist = [
            # ("mnist_testing.csv.gz", "mnist_testing.csv.gz",    600), 
            # ("a.csv", "b.csv", 60),
            # ("mnist_testing.csv.gz", "mnist_testing.csv.gz",    600), 
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
        ]

        trial = 0
        for (trainCsvFilename, testCsvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            testKey2 = testCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath + "/" + testCsvFilename,
                hex_key=testKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", testCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            print "We won't use this pruning of x on test data. See if it prunes the same as the training"
            y = 0 # first column is pixel value
            print "y:"
            # x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)

            # PARSE train****************************************
            trainKey2 = trainCsvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=importFolderPath + "/" + trainCsvFilename, schema='local',
                hex_key=trainKey2, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", trainCsvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # RF+RFView (train)****************************************
            print "This is the 'ignore=' we'll use"
            ignore_x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300, returnIgnoreX=True)

            params = {
                'response': 'C' + str(y+1),
                'cols': None,
                'ignored_cols_by_name': ignore_x,
                'classification': 1,
                'validation': None,
                'ntrees': 2,
                'max_depth': 20,
                'min_rows': None,
                'nbins': 1000,
                'mtries': None,
                'sample_rate': 0.66,
                'seed': None,

                }

        rfViewInitial = []
        for jobDispatch in range(1):
            # adjust timeoutSecs with the number of trees
            # seems ec2 can be really slow
            params['destination_key'] = 'RFModel_' + str('jobDispatch')
            kwargs = params.copy()
            timeoutSecs = 1200

            start = time.time()
            rfResult = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, noPoll=not DO_POLL, rfView=DO_POLL, **kwargs)
            elapsed = time.time() - start

            # print h2o.dump_json(rfResult)
            print "rf job dispatch end on ", trainCsvFilename, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch
            # FIX! are these already in there?
            rfView = {}
            rfView['data_key'] = trainKey2
            rfView['model_key'] = kwargs['destination_key']
            rfView['ntrees'] = kwargs['ntrees']
            rfViewInitial.append(rfView)

            if not DO_POLL:
                h2o_jobs.pollStatsWhileBusy(timeoutSecs=1200, pollTimeoutSecs=120, retryDelaySecs=5)

        # FIX! need to add the rfview and predict stuff
        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        print "rfViewInitial", rfViewInitial
        for rfView in rfViewInitial:
            print "Checking completed job:", rfView
            print "rfView", h2o.dump_json(rfView)
            data_key = rfView['data_key']
            model_key = rfView['model_key']
            ntrees = rfView['ntrees']

            rfView = h2o_cmd.runRFView(None, model_key=model_key, timeoutSecs=60, noPoll=not DO_POLL, doSimpleCheck=False)
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)
            self.assertAlmostEqual(classification_error, 20, delta=2, msg="Classification error %s differs too much" % classification_error)


            if not DO_POLL:
                h2o_jobs.pollStatsWhileBusy(timeoutSecs=300, pollTimeoutSecs=120, retryDelaySecs=5)
            # rfView = h2o_cmd.runRFView(None, data_key, model_key, timeoutSecs=60, noPoll=True, doSimpleCheck=False)
            # print "rfView:", h2o.dump_json(rfView)

            # "N":1,
            # "errs":[0.25,0.1682814508676529],
            # "testKey":"syn_binary_10000x10.hex",
            # "cm":[[3621,1399],[1515,3465]]}}
            rf_model = rfView['drf_model']
            cms = rf_model['cms']
            errs = rf_model['errs']

            # FIX! should update this expected classification error
            ## (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntrees)
            ## self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=data_key)


if __name__ == '__main__':
    h2o.unit_main()
