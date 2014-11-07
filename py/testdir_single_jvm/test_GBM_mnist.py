import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs, h2o_gbm

DO_CLASSIFICATION=True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_mnist_fvec(self):
        importFolderPath = "mnist"
        csvFilename = "mnist_training.csv.gz"
        timeoutSecs=1800
        trialStart = time.time()

        # PARSE train****************************************
        trainKey = csvFilename + "_" + ".hex"
        start = time.time()
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets',  path=importFolderPath + "/" + csvFilename, schema='put',
            hex_key=trainKey, timeoutSecs=timeoutSecs)

        elapsed = time.time() - start
        print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
        print "parse result:", parseResult['destination_key']

        # GBM (train)****************************************
        modelKey = "GBM_model"
        params = { 
            'classification': 1, # faster? 
            'destination_key': modelKey,
            'learn_rate': .1,
            'ntrees': 3,
            'max_depth': 8,
            'min_rows': 1,
            'response': 0, # this dataset has the response in the last col (0-9 to check)
            # 'ignored_cols_by_name': range(200,784) # only use the first 200 for speed?
            }

        kwargs = params.copy()
        timeoutSecs = 1800
        #noPoll -> False when GBM finished
        start = time.time()
        GBMFirstResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True, **kwargs)
        h2o_jobs.pollStatsWhileBusy(timeoutSecs=1200, pollTimeoutSecs=120, retryDelaySecs=5)
        elapsed = time.time() - start

        print "GBM training completed in", elapsed, "seconds.", \
            "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

        gbmTrainView = h2o_cmd.runGBMView(model_key=modelKey)
        errsLast = gbmTrainView['gbm_model']['errs'][-1]

        print "GBM 'errsLast'", errsLast
        if DO_CLASSIFICATION:
            cms = gbmTrainView['gbm_model']['cms']
            cm = cms[-1]['_arr'] # use the last one
            print "GBM cms[-1]['_predErr']:", cms[-1]['_predErr']
            print "GBM cms[-1]['_classErr']:", cms[-1]['_classErr']
            pctWrongTrain = h2o_gbm.pp_cm_summary(cm);
            print "\nTrain\n==========\n"
            print h2o_gbm.pp_cm(cm)
        else:
            print "GBMTrainView:", h2o.dump_json(gbmTrainView['gbm_model']['errs'])


if __name__ == '__main__':
    h2o.unit_main()
