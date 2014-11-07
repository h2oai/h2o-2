import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_import as h2i, h2o_jobs

paramDict = {
    'response': [None,'C55'],
    'max_depth': [None, 1,10,20,100],
    'nbins': [None,5,10,100,1000],
    'ignored_cols_by_name': [None,'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    'mtries': [None,1,3,5,7,9,11,13,17,19,23,37,51],
    'balance_classes': [0],
    'importance': [0],
    }

print "Will RF train on one dataset, test on another (multiple params)"


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_change_data_key_fvec(self):
        importFolderPath = 'standard'

        csvFilenameTrain = 'covtype.data'
        csvPathname = importFolderPath + "/" + csvFilenameTrain
        parseResultTrain = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=500)
        h2o_cmd.runInspect(key=parseResultTrain['destination_key'])
        dataKeyTrain = parseResultTrain['destination_key']
        print "Parse end", dataKeyTrain

        # we could train on covtype, and then use covtype20x for test? or vice versa
        # parseResult = parseResult
        # dataKeyTest = dataKeyTrain
        csvFilenameTest = 'covtype20x.data'
        csvPathname = importFolderPath + "/" + csvFilenameTest
        parseResultTest = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=500)
        print "Parse result['destination_key']:", parseResultTest['destination_key']
        inspect = h2o_cmd.runInspect(key=parseResultTest['destination_key'])
        dataKeyTest = parseResultTest['destination_key']

        print "Parse end", dataKeyTest

        # train
        # this does RFView to understand when RF completes, so the time reported for RFView here, should be 
        # considered the "first RFView" times..subsequent have some caching?. 
        # unless the no_confusion_matrix works

        # params is mutable. This is default.
        params = {
            'ntrees': 2,
            'destination_key': 'RF_model'
        }

        # colX = h2o_rf.pickRandRfParams(paramDict, params)
        kwargs = params.copy()
        kwargs["response"] = "C55"
        # adjust timeoutSecs with the number of trees
        # seems ec2 can be really slow

        timeoutSecs = 100
        start = time.time()
        h2o_cmd.runSpeeDRF(parseResult=parseResultTrain,
                            timeoutSecs=timeoutSecs, retryDelaySecs=1, noPoll=True, **kwargs)
        print "rf job dispatch end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'
        ### print "rf response:", h2o.dump_json(rfv)


        start = time.time()
        h2o_jobs.pollWaitJobs(pattern='RF_model', timeoutSecs=360, pollTimeoutSecs=120, retryDelaySecs=5)
        print "rf job end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        print "\nRFView start after job completion"
        model_key = kwargs['destination_key']
        ntrees = kwargs['ntrees']
        start = time.time()
        h2o_cmd.runSpeeDRFView(None, model_key, timeoutSecs)
        print "First rfview end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        for trial in range(3):
            # scoring
            start = time.time()
            rfView = h2o_cmd.runSpeeDRFView(None,  model_key,timeoutSecs)
            print "rfview", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'
            rfView["drf_model"] = rfView.pop("speedrf_model")
            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntrees)
            # FIX! should update this expected classification error
            # self.assertAlmostEqual(classification_error, 0.03, delta=0.5, msg="Classification error %s differs too much" % classification_error)
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest)
            print "predict", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
