import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o_browse as h2b, h2o_gbm

FORCE_FAIL_CASE = True

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_manyfiles_train_test(self):
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        if localhost:
            files = [
                # None forces num_cols to be used. assumes you set it from Inspect
                # problems with categoricals not in the train data set? (warnings in h2o stdout)
                ## ('manyfiles-nflx-gz', 'file_1.dat.gz', 'file_1.hex', 1800, None, 'file_11.dat.gz', 'file_1_test.hex')
                # just use matching
                ('manyfiles-nflx-gz', 'file_1.dat.gz', 'file_1.hex', 1800, None, 'file_1.dat.gz', 'file_1_test.hex')
                ]
        else:
            files = [
                # None forces num_cols to be used. assumes you set it from Inspect
                ('manyfiles-nflx-gz', 'file_[0-9].dat.gz', 'file_10.hex', 1800, None, 'file_1[0-9].dat.gz', 'file_10_test.hex')
                ]

        # if I got to hdfs, it's here
        # hdfs://192.168.1.176/datasets/manyfiles-nflx-gz/file_99.dat.gz

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
            csvPathname = importFolderPath + "/" + trainFilename
            parseTrainResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
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

            ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

            # if you set beta_features here, the fvec translate will happen with the Inspect not the GBM
            # h2o.beta_features = True
            inspect = h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']

            # Make col 378 it something we can do binomial regression on!
            execExpr = '%s=colSwap(%s,378,(%s[378]>15 ? 1 : 0))' % (trainKey, trainKey, trainKey)
            resultExec = h2o_cmd.runExec(expression=execExpr)

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

            # Make col 378 it something we can do binomial regression on!
            execExpr = '%s=colSwap(%s,378,(%s[378]>15 ? 1 : 0))' % (testKey, testKey, testKey)
            resultExec = h2o_cmd.runExec(expression=execExpr)

            # Note ..no inspect of test data here..so translate happens later?

            # GBM (train iterate)****************************************
            # if not response:
            #     response = num_cols - 1
            response = 378

            # randomly ignore a bunch of cols, just to make it go faster
            x = range(num_cols)
            del x[response]
            ignored_cols_by_name = random.sample(x, 300)

            print "Using the same response %s for train and test (which should have a output value too)" % response

            ntrees = 10
            # ignore 200 random cols (not the response)
            for max_depth in [5, 40]:
                params = {
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'response': response,
                    'ignored_cols_by_name': ignored_cols_by_name,
                }


                if FORCE_FAIL_CASE:
                    params = {'learn_rate': 0.2, 'classification': None, 'min_rows': 10, 'ntrees': 10, 'response': 378, 'nbins': 1024, 'ignored_cols_by_name': [256, 382, 399, 50, 176, 407, 375, 113, 170, 313, 364, 33, 361, 426, 121, 371, 232, 327, 480, 75, 37, 312, 225, 195, 244, 406, 268, 230, 321, 257, 274, 197, 35, 501, 360, 72, 213, 79, 1, 466, 362, 160, 444, 437, 5, 59, 108, 454, 73, 374, 509, 337, 183, 252, 21, 314, 100, 200, 159, 379, 405, 367, 432, 181, 8, 420, 118, 284, 281, 465, 456, 359, 291, 330, 258, 523, 243, 487, 408, 392, 15, 231, 482, 481, 70, 171, 182, 31, 409, 492, 471, 53, 45, 448, 83, 527, 452, 350, 423, 93, 447, 130, 126, 54, 354, 169, 253, 49, 42, 431, 305, 498, 216, 189, 508, 122, 308, 228, 190, 293, 451, 63, 133, 304, 397, 425, 333, 19, 158, 391, 153, 282, 112, 64, 502, 7, 16, 469, 163, 136, 40, 99, 302, 264, 325, 434, 187, 311, 286, 278, 179, 109, 348, 287, 467, 400, 164, 384, 422, 43, 117, 91, 276, 211, 175, 329, 541, 438, 145, 534, 218, 177, 317, 222, 210, 162, 402, 98, 299, 245, 385, 233, 188, 516, 143, 13, 532, 429, 172, 455, 470, 518, 236, 296, 388, 468, 110, 395, 185, 25, 489, 196, 120, 435, 165, 168, 271, 74, 510, 36, 76, 208, 223, 270, 515, 421, 87, 66, 473, 220, 46, 486, 102, 38, 156, 48, 132, 331, 51, 403, 234, 23, 449, 341, 303, 410, 479, 203, 413, 512, 513, 9, 446, 511, 55, 6, 339, 418, 476, 178, 266, 22, 141, 259, 349, 86, 144, 34, 290, 326, 318, 519, 424, 127, 174, 472, 116, 17, 152, 280, 215, 514, 103, 377, 537, 373, 238, 47, 353, 428, 94, 214, 61, 123, 386, 351, 246, 411, 101, 249, 240, 520, 307, 288, 199, 147, 436, 77, 464, 414], 'source': u'file_1.hex', 'validation': u'file_1.hex', 'max_depth': 5} 

                ### print "Using these parameters for GBM: ", params
                kwargs = params.copy()
                h2o.beta_features = True

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

                cm = gbmTrainView['gbm_model']['cm']
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
                # hack
                if h2o.beta_features:
                    h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                elapsed = time.time() - start
                print "GBM predict completed in", elapsed, "seconds. On dataset: ", testFilename

                print "This is crazy!"
                gbmPredictCMResult =h2o.nodes[0].predict_confusion_matrix(
                    actual=parseTestResult['destination_key'],
                    vactual=response,
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

            h2o.beta_features = False
            xLabel = 'max_depth'
            eLabel = 'pctWrong'
            fLabel = 'trainElapsed'
            eListTitle = ""
            fListTitle = ""
            h2o_gbm.plotLists(xList, xLabel, eListTitle, eList, eLabel, fListTitle, fList, fLabel)

if __name__ == '__main__':
    h2o.unit_main()
