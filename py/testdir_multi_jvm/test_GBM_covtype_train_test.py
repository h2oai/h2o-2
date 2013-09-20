import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o_browse as h2b

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
            for max_depth in [5,10,20]:
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
                cm = gbmTrainView['gbm_model']['cm']
                print "GBM 'cm'", cm
                print "GBM 'errsLast'", errsLast

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

                # gbmTestView = h2o_cmd.runGBMView(model_key=modelKey)
                gbmTestView = gbmPredictCMResult
                ### print "gbmTestView:", h2o.dump_json(gbmTestView)

                # errrs from end of list? is that the last tree?
                # errsLast = gbmTestView['gbm_model']['errs'][-1]

                # all we get is cm
                cm = gbmPredictCMResult['cm']

                print "GBM 'errsLast'", errsLast

                # hack cut and past for now (should be in h2o_gbm.py?
                scoresList = cm
                totalScores = 0
                totalRight = 0
                # individual scores can be all 0 if nothing for that output class
                # due to sampling
                classErrorPctList = []
                predictedClassDict = {} # may be missing some? so need a dict?
                for classIndex,s in enumerate(scoresList):
                    classSum = sum(s)
                    if classSum == 0 :
                        # why would the number of scores for a class be 0? does RF CM have entries for non-existent classes
                        # in a range??..in any case, tolerate. (it shows up in test.py on poker100)
                        print "class:", classIndex, "classSum", classSum, "<- why 0?"
                    else:
                        # H2O should really give me this since it's in the browser, but it doesn't
                        classRightPct = ((s[classIndex] + 0.0)/classSum) * 100
                        totalRight += s[classIndex]
                        classErrorPct = 100 - classRightPct
                        classErrorPctList.append(classErrorPct)
                        ### print "s:", s, "classIndex:", classIndex
                        print "class:", classIndex, "classSum", classSum, "classErrorPct:", "%4.2f" % classErrorPct

                        # gather info for prediction summary
                        for pIndex,p in enumerate(s):
                            if pIndex not in predictedClassDict:
                                predictedClassDict[pIndex] = p
                            else:
                                predictedClassDict[pIndex] += p

                    totalScores += classSum

                print "Predicted summary:"
                # FIX! Not sure why we weren't working with a list..hack with dict for now
                for predictedClass,p in predictedClassDict.items():
                    print str(predictedClass)+":", p

                # this should equal the num rows in the dataset if full scoring? (minus any NAs)
                print "totalScores:", totalScores
                print "totalRight:", totalRight
                if totalScores != 0:  pctRight = 100.0 * totalRight/totalScores
                else: pctRight = 0.0
                print "pctRight:", "%5.2f" % pctRight
                pctWrong = 100 - pctRight
                print "pctWrong:", "%5.2f" % pctWrong

                # xList.append(ntrees)
                xList.append(max_depth)
                eList.append(pctWrong)
                sList.append(trainElapsed)

            h2o.beta_features = False
            plotit(xList, eList, sList)

if __name__ == '__main__':
    h2o.unit_main()
