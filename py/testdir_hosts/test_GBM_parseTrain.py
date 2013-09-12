import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i, h2o_glm, h2o_util, h2o_rf
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBM_parseTrain(self):
        #folderpath, filename, keyname, timeout
        bucket = 'home-0xdiag-datasets'
        files = [('mnist/mnist8m', 'mnist8m-train-1.csv', 'mnist8mtrain.hex', 1800, 0),
                 ('airlines', '1988_2008.csv', 'airlines.hex', 1800,'LateAircraftDelay'),
                 ('standard', 'covtype200x.data', 'covtype.hex', 1800, 55)
                ]
        for importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse in files:
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
                'ntrees':10,
                'max_depth':8,
                'min_rows':1,
                'vresponse':vresponse
            }   

            kwargs = params.copy()
            h2o.beta_features = True
            #noPoll -> False when GBM finished
            GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
            print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
                "%f pct. of timeout" % (GBMResult['python_%timeout'])

if __name__ == '__main__':
    h2o.unit_main()

