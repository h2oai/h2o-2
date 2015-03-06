import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o_browse as h2b, h2o_gbm

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=14)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_covtype_train_test(self):
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        files = [
                ('standard', 'covtype.shuffled.90pct.data', 'covtype.train.hex', 1800, 'C55', 'covtype.shuffled.10pct.data', 'covtype.test.hex')
                ]

        # h2b.browseTheCloud()
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
            # make the response column categorical
            # is the column index 1-base in to_enum
            result = h2o.nodes[0].to_enum(None, src_key=trainKey, column_index=54+1)

            # Parse (test)****************************************
            parseTestResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + testFilename, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "test parse end on ", testFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "test parse result:", parseTestResult['destination_key']
            result = h2o.nodes[0].to_enum(None, src_key=testKey, column_index=54+1)

            # GBM (train iterate)****************************************
            inspect = h2o_cmd.runInspect(key=parseTestResult['destination_key'])
<<<<<<< HEAD
            ntrees = 2
            # fails with 40 in the past
            for trial in range(9):
                imp = trial % 2
                # None means use h2o default
                if imp == 2:
                    imp = None
                
                max_depth = 10
=======
            ntrees = 3
            for trial in range(9):
                imp = trial % 2
                # None will cause the h2o default for importance. otherwise: false, true
                # what the heck, change group_split at the same time
                if imp==2:
                    importance = None
                    group_split = None
                else:
                    importance = imp
                    group_split = imp

                max_depth = 10
                # use defaults?
>>>>>>> 50f5b1b8c94b6ce7cd5ec175fecdca811f41487f
                params = {
                    # 'learn_rate': .2,
                    # 'nbins': 20,
                    'ntrees': ntrees,
                    # 'max_depth': max_depth,
                    # 'min_rows': 2,
                    'response': response,
<<<<<<< HEAD
                    'ignored_cols_by_name': None,
                    'importance': imp, # h2o defaults to 1 now?
                    'classification': 1,
=======
                    # 'ignored_cols_by_name': None,
                    'importance': importance,
                    'group_split': group_split,
>>>>>>> 50f5b1b8c94b6ce7cd5ec175fecdca811f41487f
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                # translate it (only really need to do once . out of loop?
                h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
                ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

                # GBM train****************************************
                trainStart = time.time()
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult, timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
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
                xList.append(trial)
                eList.append(pctWrong)
                fList.append(trainElapsed)

<<<<<<< HEAD
            xLabel = 'trial'
=======
            xLabel = 'trial. importance=0,1,default,...'
>>>>>>> 50f5b1b8c94b6ce7cd5ec175fecdca811f41487f
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
