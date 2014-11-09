import unittest
import random, sys, time, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_parseTrain(self):
        bucket = 'home-0xdiag-datasets'
       
        files = [('standard', 'covtype.data', 'covtype.hex', 1800, 54)
                ]
                  
        for importFolderPath,csvFilename,trainKey,timeoutSecs,response in files:
            # PARSE train****************************************
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
                hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # GBM (train)****************************************
            params = { 
                'destination_key': "GBMKEY",
                'learn_rate':.1,
                'ntrees':1,
                'max_depth':1,
                'min_rows':1,
                'response':response
            }   
            print "Using these parameters for GBM: ", params
            kwargs = params.copy()
            #noPoll -> False when GBM finished
            GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
            h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
            #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
            #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
            GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
            print GBMView['gbm_model']['errs']

if __name__ == '__main__':
    h2o.unit_main()

