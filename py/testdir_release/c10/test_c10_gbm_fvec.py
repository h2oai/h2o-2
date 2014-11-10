import unittest, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_common, h2o_print, h2o_glm, h2o_jobs as h2j, h2o_gbm

print "Assumes you ran ../../cloud.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"

print "Uses 0xcust.. data. Cloud must be built as 0xcust.. and run with access to 0xdata /mnt automounts"
print "Assume there are links in /home/0xcust.. to the nas bucket used"
print "i.e. /home/0xcust... should have results of 'ln -s /mnt/0xcustomer-datasets"
print "The path resolver in python tests will find it in the home dir of the username being used"
print "to run h2o..i.e from the config json which builds the cloud and passes that info to the test"
print "via the cloned cloud mechanism (h2o-nodes.json)"

DO_PREDICT_CM = False
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c10_rel_gbm(self):
        print "not necessarily run as user=0xcust..., can't use a  schema='put' here"
        print "Want to be able to run python as jenkins"
        print "I guess for big 0xcust files, we don't need schema='put'"
        print "For files that we want to put (for testing put), we can get non-private files"

        # Parse Test***********************************************************
        importFolderPath = '/mnt/0xcustomer-datasets/c3'
        testFilename = 'classification1Test.txt'
        testPathname = importFolderPath + "/" + testFilename

        start = time.time()
        parseTestResult = h2i.import_parse(path=testPathname, schema='local', timeoutSecs=500, doSummary=True)
        print "Parse of", parseTestResult['destination_key'], "took", time.time() - start, "seconds"

        # Parse Train***********************************************************
        importFolderPath = '/mnt/0xcustomer-datasets/c3'
        trainFilename = 'classification1Train.txt'
        trainPathname = importFolderPath + "/" + trainFilename

        start = time.time()
        parseTrainResult = h2i.import_parse(path=trainPathname, schema='local', 
            timeoutSecs=500, doSummary=True)
        print "Parse of", parseTrainResult['destination_key'], "took", time.time() - start, "seconds"

        start = time.time()
        inspect = h2o_cmd.runInspect(None, parseTrainResult['destination_key'], timeoutSecs=500)
        print "Inspect:", parseTrainResult['destination_key'], "took", time.time() - start, "seconds"
        h2o_cmd.infoFromInspect(inspect, trainPathname)
        # num_rows = inspect['num_rows']
        # num_cols = inspect['num_cols']
        # do summary of the parsed dataset last, since we know it fails on this dataset
        summaryResult = h2o_cmd.runSummary(key=parseTrainResult['destination_key'])
        h2o_cmd.infoFromSummary(summaryResult, noPrint=False)

        # GBM Train***********************************************************
        x = [6,7,8,10,12,31,32,33,34,35,36,37,40,41,42,43,44,45,46,47,49,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70]
        # response = 0
        # doesn't work if index is used?
        response = 'outcome'

        # x = range(inspect['num_cols'])
        # del x[response]
        ntrees = 10
        # fails with 40
        params = {
            'learn_rate': .2,
            'nbins': 1024,
            'ntrees': ntrees,
            'max_depth': 20,
            'min_rows': 2,
            'response': response,
            'cols': x,
            # 'ignored_cols_by_name': None,
        }
        print "Using these parameters for GBM: ", params
        kwargs = params.copy()
        modelKey = 'GBMModelKey'

        timeoutSecs = 900

        trainStart = time.time()
        gbmTrainResult = h2o_cmd.runGBM(parseResult=parseTrainResult,
            timeoutSecs=timeoutSecs, destination_key=modelKey, **kwargs)
        trainElapsed = time.time() - trainStart
        print "GBM training completed in", trainElapsed, "seconds. On dataset: ", trainFilename

        gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
        # errrs from end of list? is that the last tree?
        errsLast = gbmTrainView['gbm_model']['errs'][-1]
        print "GBM 'errsLast'", errsLast

        # get the last cm
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


        if DO_PREDICT_CM:
            gbmPredictCMResult =h2o.nodes[0].predict_confusion_matrix(
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

if __name__ == '__main__':
    h2o.unit_main()
