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
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_manyfiles_train_test(self):
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        if h2o.localhost:
            files = [
                # None forces numCols to be used. assumes you set it from Inspect
                # problems with categoricals not in the train data set? (warnings in h2o stdout)
                ## ('manyfiles-nflx-gz', 'file_1.dat.gz', 'file_1.hex', 1800, None, 'file_11.dat.gz', 'test.hex')
                # just use matching
                ('manyfiles-nflx-gz', 'file_1.dat.gz', 'train.hex', 1800, None, 'file_1.dat.gz', 'test.hex')
                ]
        else:
            files = [
                # None forces numCols to be used. assumes you set it from Inspect
                ('manyfiles-nflx-gz', 'file_[0-9].dat.gz', 'train.hex', 1800, None, 'file_1[0-9].dat.gz', 'test.hex')
                ]

        # if I got to hdfs, it's here
        # hdfs://172.16.2.176/datasets/manyfiles-nflx-gz/file_99.dat.gz

        h2b.browseTheCloud()
        for (importFolderPath, trainFilename, trainKey, timeoutSecs, response, testFilename, testKey) in files:
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            # Parse (train)****************************************
            csvPathname = importFolderPath + "/" + trainFilename
            parseTrainResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "train parse end on ", trainFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

            inspect = h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            # Make col 378 it something we can do binomial regression on!
            execExpr = '%s[,378+1]=%s[,378+1]>15' % (trainKey, trainKey)
            resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=300)

            # Parse (test)****************************************
            parseTestResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + testFilename, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs, doSummary=False)
            elapsed = time.time() - start
            print "test parse end on ", testFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "test parse result:", parseTestResult['destination_key']

            # Make col 378 it something we can do binomial regression on!
            execExpr = '%s[,378+1]=%s[,378+1]>15' % (testKey, testKey)
            resultExec = h2o_cmd.runExec(str=execExpr, timeoutSecs=300)

            # Note ..no inspect of test data here..so translate happens later?

            # GBM (train iterate)****************************************
            # if not response:
            #     response = numCols - 1
            response = 378

            # randomly ignore a bunch of cols, just to make it go faster
            x = range(numCols)
            del x[response]
            ignored_cols_by_name = ",".join(map(lambda x: 'C' + str(x+1), random.sample(x, 300)))

            print "Using the same response %s for train and test (which should have a output value too)" % "C" + str(response+1)

            ntrees = 10
            # ignore 200 random cols (not the response)
            for max_depth in [5, 40]:
                params = {
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'response': 'C' + str(response+1),
                    'ignored_cols_by_name': ignored_cols_by_name,
                }
            



                ### print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                # GBM train****************************************
                trainStart = time.time()
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
                    timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
                # hack
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
                ### h2o_cmd.runInspect(key=parseTestResult['destination_key'])
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
                    vactual='C' + str(response+1),
                    predict=predictKey,
                    vpredict='predict', # choices are 0 and 'predict'
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
                xList.append(max_depth)
                eList.append(pctWrong)
                fList.append(trainElapsed)

            xLabel = 'max_depth'
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
