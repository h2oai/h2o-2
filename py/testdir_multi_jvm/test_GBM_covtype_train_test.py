import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o_browse as h2b, h2o_gbm

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=28)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_covtype_train_test(self):
        h2o.beta_features = False
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        files = [
                ('standard', 'covtype.shuffled.90pct.data', 'covtype.train.hex', 1800, 'C55', 'covtype.shuffled.10pct.data', 'covtype.test.hex')
                ]

        # h2b.browseTheCloud()

        for (importFolderPath, trainFilename, trainKey, timeoutSecs, response, testFilename, testKey) in files:
            h2o.beta_features = False #turn off beta_features
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            # Parse (train)****************************************
            if h2o.beta_features:
                print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            parseTrainResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + trainFilename, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs, noPoll=h2o.beta_features, doSummary=False)
            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                print "Filling in the parseTrainResult['destination_key'] for h2o"
                parseTrainResult['destination_key'] = trainKey

            elapsed = time.time() - start
            print "train parse end on ", trainFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            # Parse (test)****************************************
            if h2o.beta_features:
                print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            parseTestResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + testFilename, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs, noPoll=h2o.beta_features, doSummary=False)
            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                print "Filling in the parseTestResult['destination_key'] for h2o"
                parseTestResult['destination_key'] = testKey

            elapsed = time.time() - start
            print "test parse end on ", testFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "test parse result:", parseTestResult['destination_key']

            # GBM (train iterate)****************************************
            inspect = h2o_cmd.runInspect(key=parseTestResult['destination_key'])
            ntrees = 2
            # fails with 40
            for max_depth in [40, 5]:
                params = {
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'response': response,
                    'ignored_cols_by_name': None,
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                h2o.beta_features = True
                # translate it (only really need to do once . out of loop?
                h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
                ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

                # GBM train****************************************
                trainStart = time.time()
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
                    noPoll=True, timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
                # hack
                if h2o.beta_features:
                    h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                trainElapsed = time.time() - trainStart
                print "GBM training completed in", trainElapsed, "seconds. On dataset: ", trainFilename

                gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
                # errrs from end of list? is that the last tree?
                errsLast = gbmTrainView['gbm_model']['errs'][-1]
                print "GBM 'errsLast'", errsLast

                cm = gbmTrainView['gbm_model']['cms'][-1]['_arr']
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
                # hack
                if h2o.beta_features:
                    h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                elapsed = time.time() - start
                print "GBM predict completed in", elapsed, "seconds. On dataset: ", testFilename

                gbmPredictCMResult =h2o.nodes[0].predict_confusion_matrix(
                    actual=parseTestResult['destination_key'],
                    vactual=response,
                    predict=predictKey,
                    vpredict='predict', # choices are 7 (now) and 'predict'
                    )

                # errrs from end of list? is that the last tree?
                # all we get is cm
                cm = gbmPredictCMResult['cm']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                print "\nTest\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(pctWrong)
                fList.append(trainElapsed)

            h2o.beta_features = False
            xLabel = 'max_depth'
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
