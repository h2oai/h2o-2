import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o_browse as h2b, h2o_gbm

def plotit(xList, eList, sList):
    if h2o.python_username!='kevin':
        return
    
    import pylab as plt
    if eList:
        print "xList", xList
        print "eList", eList
        print "sList", sList

        font = {'family' : 'normal',
                'weight' : 'normal',
                'size'   : 26}
        ### plt.rc('font', **font)
        plt.rcdefaults()

        label = "1jvmx28GB covtype train 90/test 10 GBM learn_rate=.2 nbins=1024 ntrees=40 min_rows = 10"
        plt.figure()
        plt.plot (xList, eList)
        plt.xlabel('max_depth')
        plt.ylabel('pctWrong')
        plt.title(label)
        plt.draw()

        label = "1jvmx28GB Covtype GBM learn_rate=.2 nbins=1024 ntrees=40 min_rows = 10"
        plt.figure()
        plt.plot (xList, sList)
        plt.xlabel('max_depth')
        plt.ylabel('time')
        plt.title(label)
        plt.draw()

        plt.show()


import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=28)
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
                ('standard', 'covtype.shuffled.90pct.data', 'covtype.train.hex', 1800, 54, 'covtype.shuffled.10pct.data', 'covtype.test.hex')
                ]

        # h2b.browseTheCloud()

        for (importFolderPath, trainFilename, trainKey, timeoutSecs, vresponse, testFilename, testKey) in files:
            h2o.beta_features = False #turn off beta_features
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            sList = []

            # Parse (train)****************************************
            print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            parseTrainResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + trainFilename, schema='local',
                hex_key=trainKey, timeoutSecs=timeoutSecs, noPoll=h2o.beta_features, doSummary=False)
            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(timeoutSecs=1800, pollTimeoutSecs=1800)
                print "Filling in the parseTrainResult['destination_key'] for h2o"
                parseTrainResult['destination_key'] = trainKey

            elapsed = time.time() - start
            print "train parse end on ", trainFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "train parse result:", parseTrainResult['destination_key']

            # Parse (test)****************************************
            print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            parseTestResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + testFilename, schema='local',
                hex_key=testKey, timeoutSecs=timeoutSecs, noPoll=h2o.beta_features, doSummary=False)
            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(timeoutSecs=1800, pollTimeoutSecs=1800)
                print "Filling in the parseTestResult['destination_key'] for h2o"
                parseTestResult['destination_key'] = testKey

            elapsed = time.time() - start
            print "test parse end on ", testFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "test parse result:", parseTestResult['destination_key']

            # GBM (train)****************************************
            # for depth in [5]:
            # depth = 5
            # for ntrees in [10,20,40,80,160]:
            ntrees = 40
            ntrees = 10
            for max_depth in [5,10,20,40]:
            # for max_depth in [5,10,20,40]:
            # for ntrees in [1,2,3,4]:
                params = {
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': ntrees,
                    'max_depth': max_depth,
                    'min_rows': 10,
                    'vresponse': vresponse,
                    # 'ignored_cols': 
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()

                h2o.beta_features = True
                # translate it
                h2o_cmd.runInspect(key=parseTrainResult['destination_key'])
                ### h2o_cmd.runSummary(key=parsTraineResult['destination_key'])

                # GBM train****************************************
                trainStart = time.time()
                gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
                    noPoll=True, timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
                # hack
                if h2o.beta_features:
                    h2j.pollWaitJobs(timeoutSecs=1800, pollTimeoutSecs=1800)
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
                h2o_cmd.runInspect(key=parseTestResult['destination_key'])
                start = time.time()
                gbmTestResult = h2o_cmd.runPredict(
                    data_key=parseTestResult['destination_key'], 
                    model_key=modelKey,
                    destination_key=predictKey,
                    timeoutSecs=timeoutSecs, **kwargs)
                # hack
                if h2o.beta_features:
                    h2j.pollWaitJobs(timeoutSecs=1800, pollTimeoutSecs=1800)
                elapsed = time.time() - start
                print "GBM predict completed in", elapsed, "seconds. On dataset: ", testFilename

                print "This is crazy!"
                gbmPredictCMResult =h2o.nodes[0].predict_confusion_matrix(
                    actual=parseTestResult['destination_key'],
                    vactual=vresponse,
                    predict=predictKey,
                    vpredict='predict', # choices are 0 and 'predict'
                    )

                # errrs from end of list? is that the last tree?
                # all we get is cm
                cm = gbmPredictCMResult['cm']

                # These will move into the h2o_gbm.py
                pctWrong = h2o_gbm.pp_cm_summary(cm);
                print "Last line of this cm is really NAs, not CM"
                # print "\nTest\n==========\n{0}".format
                print "\nTest\n==========\n"
                print h2o_gbm.pp_cm(cm)

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(pctWrong)
                sList.append(trainElapsed)

            h2o.beta_features = False
            plotit(xList, eList, sList)


if __name__ == '__main__':
    h2o.unit_main()
