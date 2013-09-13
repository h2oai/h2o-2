import unittest
import random, sys, time, re, itertools
from pprint import pprint
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i, h2o_glm, h2o_util, h2o_rf,h2o_jobs as h2j
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
        
        files = [('mnist', 'mnist_training.csv.gz', 'mnistsmalltrain.hex',1800,0)
                # ('manyfiles-nflx-gz', 'file_95.dat.gz', 'nflx.hex',1800,541),
                ]
        ntrees = 2
        learn_rate = 2
        max_depth = 2
        grid = [range(1,ntrees), [(i+0.0)/learn_rate for i in range(1,learn_rate + 1)], range(1,max_depth)] 
        grid = list(itertools.product(*grid))
        for importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse in files:
            # PARSE train****************************************
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
                hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key'] 
            for ntree, learn_rate, max_depth in grid:
                params = {
                 'destination_key': 'GBMKEY',
                 'learn_rate': learn_rate,
                 'ntrees':ntree,
                 'max_depth':max_depth,
                 'min_rows':1,
                 'vresponse':vresponse
                 }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()
                h2o.beta_features = True
                #noPoll -> False when GBM finished
                GBMResult = h2o_cmd.runGBM(parseResult=parseResult,noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
                h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
                #print "GBM training completed in", GBMResult['python_elapsed'], "seconds.", \
                #    "%f pct. of timeout" % (GBMResult['python_%timeout'])
                #print GBMResult
                GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
                pprint(GBMView)

if __name__ == '__main__':
    h2o.unit_main()
