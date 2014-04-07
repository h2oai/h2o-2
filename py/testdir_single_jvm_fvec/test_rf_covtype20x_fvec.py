import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_hosts, h2o_import as h2i, h2o_jobs, h2o_gbm

DO_SMALL = True
# we can pass ntree thru kwargs if we don't use the "trees" parameter in runRF
# only classes 1-7 in the 55th col
# don't allow None on ntree..causes 50 tree default!
print "Temporarily not using bin_limit=1 to 4"
print "Use out_of_bag_error_estimate=0, to get full scoring, for when we change datasets"
drf1ParamDict = {
    'response_variable': [None,54],
    'class_weights': [None,'1=2','2=2','3=2','4=2','5=2','6=2','7=2'], 'ntree': [5], 
    # UPDATE: H2O...OOBE has to be 0 for scoring
    'out_of_bag_error_estimate': [0],
    'stat_type': [None, 'ENTROPY', 'GINI'],
    'depth': [None, 1,10,20,100],
    'bin_limit': [None,5,10,100,1000],
    'ignore': [None,0,1,2,3,4,5,6,7,8,9],
    'sample': [None,20,40,60,80,90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    # stack trace if we use more features than legal. dropped or redundant cols reduce legal max also.
    # only 51 non-constant cols in the 20k covtype?
    'features': [None,1,3,5,7,9,11,13,17,19,23,37,51],
    'exclusive_split_limit': [None,0,3,5],
    'sampling_strategy': [None, 'RANDOM', 'STRATIFIED_LOCAL' ],
    'strata_samples': [
        None,
        "2=10",
        "1=5",
        "2=3",
        "1=1,2=1,3=1,4=1,5=1,6=1,7=1",
        "1=99,2=99,3=99,4=99,5=99,6=99,7=99",
        "1=0,2=0,3=0,4=0,5=0,6=0,7=0",
        ]
    }

drf2ParamDict = {
    'response': [None, 'C54'],
    'max_depth': [None, 10,20,100],
    'nbins': [None,5,10,100,1000],
    'ignored_cols_by_name': [None,'C1','C2','C3','C4','C5','C6','C7','C8','C9'],
    'sample_rate': [None,0.20,0.40,0.60,0.80,0.90],
    'seed': [None,'0','1','11111','19823134','1231231'],
    'mtries': [None,1,3,5,7,9,11,13,17,19,23,37,51],
    }

print "Will RF train on one dataset, test on another (multiple params)"
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_covtype20x_fvec(self):
        h2o.beta_features = True
        importFolderPath = 'standard'

        if DO_SMALL:
            csvFilenameTrain = 'covtype.data'
            hex_key = 'covtype1x.data.A.hex'
        else:
            csvFilenameTrain = 'covtype20x.data'
            hex_key = 'covtype20x.data.A.hex'

        csvPathname = importFolderPath + "/" + csvFilenameTrain
        parseResultTrain = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, timeoutSecs=500)
        inspect = h2o_cmd.runInspect(key=parseResultTrain['destination_key'])
        dataKeyTrain = parseResultTrain['destination_key']
        print "Parse end", dataKeyTrain

        # have to re import since source key is gone
        # we could just copy the key, but sometimes we change the test/train data  to covtype.data
        if DO_SMALL:
            csvFilenameTest = 'covtype.data'
            hex_key = 'covtype1x.data.B.hex'
            dataKeyTest2 = 'covtype1x.data.C.hex'
        else:
            csvFilenameTest = 'covtype20x.data'
            hex_key = 'covtype20x.data.B.hex'
            dataKeyTest2 = 'covtype20x.data.C.hex'

        csvPathname = importFolderPath + "/" + csvFilenameTest
        parseResultTest = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, timeoutSecs=500)
        print "Parse result['destination_key']:", parseResultTest['destination_key']
        inspect = h2o_cmd.runInspect(key=parseResultTest['destination_key'])
        dataKeyTest = parseResultTest['destination_key']
        print "Parse end", dataKeyTest

        # make a 3rd key so the predict is uncached too!
        execExpr = dataKeyTest2 + "=" + dataKeyTest
        if h2o.beta_features:
            kwargs = {'str': execExpr, 'timeoutSecs': 15}
        else:
            kwargs = {'expression': execExpr, 'timeoutSecs': 15}

        resultExec = h2o_cmd.runExec(**kwargs)

        # train
        # this does RFView to understand when RF completes, so the time reported for RFView here, should be 
        # considered the "first RFView" times..subsequent have some caching?. 
        # unless the no_confusion_matrix works

        # params is mutable. This is default.
        if h2o.beta_features:
            paramDict = drf2ParamDict
            params = {
                'ntrees': 20, 
                'destination_key': 'RF_model'
            }
        else:
            paramDict = drf1ParamDict
            params = {
                'ntree': 20, 
                'out_of_bag_error_estimate': 1, 
                'model_key': 'RF_model'
            }

        colX = h2o_rf.pickRandRfParams(paramDict, params)

        kwargs = params.copy()
        if h2o.beta_features:
            timeoutSecs = 30 + kwargs['ntrees'] * 60
        else:
            timeoutSecs = 30 + kwargs['ntree'] * 60 

        start = time.time()
        rf = h2o_cmd.runRF(parseResult=parseResultTrain,
            timeoutSecs=timeoutSecs, retryDelaySecs=1, **kwargs)
        print "rf job end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        print "\nRFView start after job completion"
        if h2o.beta_features:
            model_key = kwargs['destination_key']
            ntree = kwargs['ntrees']
        else:
            model_key = kwargs['model_key']
            ntree = kwargs['ntree']

        start = time.time()
        # this does the RFModel view for v2. but only model_key is used. Data doesn't matter? (nor ntree)
        h2o_cmd.runRFView(None, dataKeyTrain, model_key, ntree=ntree, timeoutSecs=timeoutSecs)
        print "First rfview end on ", dataKeyTrain, 'took', time.time() - start, 'seconds'

        for trial in range(1):
            # scoring
            start = time.time()
            rfView = h2o_cmd.runRFView(None, dataKeyTest, 
                model_key, ntree=ntree, timeoutSecs=timeoutSecs, out_of_bag_error_estimate=0, retryDelaySecs=1)
            print "rfview", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView, ntree=ntree)
            self.assertAlmostEqual(classification_error, 50, delta=50, 
                msg="Classification error %s differs too much" % classification_error)
            start = time.time()
            predict = h2o.nodes[0].generate_predictions(model_key=model_key, data_key=dataKeyTest2)
            print "predict", trial, "end on ", dataKeyTest, 'took', time.time() - start, 'seconds.'

            parseKey = parseResultTrain['destination_key']
            rfModelKey  = rfView['drf_model']['_key']
            predictKey = 'Predict.hex'
            start = time.time()

            predictResult = h2o_cmd.runPredict(
                data_key=parseKey,
                model_key=rfModelKey,
                destination_key=predictKey,
                timeoutSecs=timeoutSecs)

            predictCMResult = h2o.nodes[0].predict_confusion_matrix(
                actual=parseKey,
                vactual='C54',
                predict=predictKey,
                vpredict='predict',
                )

            cm = predictCMResult['cm']

            # These will move into the h2o_gbm.py
            pctWrong = h2o_gbm.pp_cm_summary(cm);
            print "\nTest\n==========\n"
            print h2o_gbm.pp_cm(cm)

            print "Trial #", trial, "completed"

if __name__ == '__main__':
    h2o.unit_main()
