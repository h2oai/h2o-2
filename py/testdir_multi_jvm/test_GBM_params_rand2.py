import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o_browse as h2b, h2o_gbm
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j

# this only covers the params for gbm
# FIX! what about the other algos below
def define_gbm_params():
    paramsDict = {
        'learn_rate': [None, 0.1, 0.2, 0.3],
        'nbins': [2, 10, 1024], # has to be between 2 and 100000
        'ntrees': [1, 2, 4, 10],
        'max_depth': [None, 1, 2, 4, 8, 40], # 0 might cause problem
        'min_rows': [None, 1, 2, 100, 10000000],
        'response': [54],
        'ignored_cols_by_name': [None, 'C1,C2,C3,C4', 'C1'],
        'classification': [None, 1], # FIX! add regression when we can predict
    }
    return paramsDict

DO_PREDICT_CM = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()

        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_params_rand2(self):
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        files = [
                # ('standard', 'covtype.shuffled.90pct.data', 'covtype.train.hex', 1800, 54, 'covtype.shuffled.10pct.data', 'covtype.test.hex')
                ('standard', 'covtype.shuffled.10pct.sorted.data', 'covtype.train.hex', 1800, 54, 'covtype.shuffled.10pct.data', 'covtype.test.hex')
                ]

        for (importFolderPath, trainFilename, trainKey, timeoutSecs, response, testFilename, testKey) in files:
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            # Parse (train)****************************************
            parseTrainResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + trainFilename, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs, doSummary=False)

            elapsed = time.time() - start
            print "train parse end on ", trainFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            # Parse (test)****************************************
            parseTestResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + testFilename, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "test parse end on ", testFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "test parse result:", parseTestResult['destination_key']

            # GBM (train iterate)****************************************
            inspect = h2o_cmd.runInspect(key=parseTestResult['destination_key'])
            paramsDict = define_gbm_params()
            for trial in range(3):
                # translate it (only really need to do once . out of loop?
                h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
                ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

                # use this to set any defaults you want if the pick doesn't set
                params = {
                    'response': 54, 
                    'ignored_cols_by_name': 'C1,C2,C3,C4,C5', 
                    'ntrees': 2,
                    'validation': parseTestResult['destination_key'],
                }
                h2o_gbm.pickRandGbmParams(paramsDict, params)
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                # GBM train****************************************
                trainStart = time.time()
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
                    timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
                trainElapsed = time.time() - trainStart
                print "GBM training completed in", trainElapsed, "seconds. On dataset: ", trainFilename

                gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
                # errrs from end of list? is that the last tree?
                errsLast = gbmTrainView['gbm_model']['errs'][-1]
                print "GBM 'errsLast'", errsLast

                cm = gbmTrainView['gbm_model']['cms'][-1]['_arr'] # use the last one
                pctWrongTrain = h2o_gbm.pp_cm_summary(cm);
                print "Last line of this cm might be NAs, not CM"
                print "\nTrain\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # GBM test****************************************
                predictKey = 'Predict.hex'
                h2o_cmd.runInspect(key=parseTestResult['destination_key'])
                start = time.time()
                gbmTestResult = h2o_cmd.runPredict(
                    data_key=parseTestResult['destination_key'], 
                    model_key=modelKey,
                    destination_key=predictKey,
                    timeoutSecs=timeoutSecs)
                elapsed = time.time() - start
                print "GBM predict completed in", elapsed, "seconds. On dataset: ", testFilename

                if DO_PREDICT_CM:
                    gbmPredictCMResult = h2o.nodes[0].predict_confusion_matrix(
                        actual=parseTestResult['destination_key'],
                        vactual='predict',
                        predict=predictKey,
                        vpredict='predict', # choices are 7 (now) and 'predict'
                        )

                    # errrs from end of list? is that the last tree?
                    # all we get is cm
                    cm = gbmPredictCMResult['cm']

                    # These will move into the h2o_gbm.py
                    pctWrong = h2o_gbm.pp_cm_summary(cm);
                    print "Last line of this cm is really NAs, not CM"
                    print "\nTest\n==========\n"
                    print h2o_gbm.pp_cm(cm)

                # xList.append(ntrees)
                if 'max_depth' in params and params['max_depth']:
                    xList.append(params['max_depth'])
                    eList.append(pctWrongTrain)
                    fList.append(trainElapsed)

            xLabel = 'max_depth'
            eLabel = 'pctWrongTrain'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
