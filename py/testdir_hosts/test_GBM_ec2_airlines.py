import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
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
        h2o.beta_features = False
        bucket = 'h2o-airlines-unpacked'  #'home-0xdiag-datasets'

        files = [#('standard', 'covtype200x.data', 'covtype.hex', 1800, 54),
                 #('mnist', 'mnist8m.csv', 'mnist8m.hex',1800,0),
                 #('manyfiles-nflx-gz', 'file_95.dat.gz', 'nflx.hex',1800,256),
                 #('standard', 'allyears2k.csv', 'allyears2k.hex',1800,'IsDepDelayed'),
                 ('', 'allyears.csv', 'allyears.hex',1800,'IsDepDelayed')
                ]

        for importFolderPath,csvFilename,trainKey,timeoutSecs,vresponse in files:
            h2o.beta_features = False #turn off beta_features
            # PARSE train****************************************
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=importFolderPath + "/" + csvFilename,
                hex_key=trainKey, timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # GBM (train)****************************************
            for depth in [5,15]:
                params = {
                    'destination_key': "GBMKEY",
                    'learn_rate': .2,
                    'nbins': 1024,
                    'ntrees': 10,
                    'max_depth': depth,
                    'min_rows': 10,
                    'vresponse': vresponse,
                    'ignored_cols': 'CRSDepTime,CRSArrTime,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed'
                }
                print "Using these parameters for GBM: ", params
                kwargs = params.copy()
                h2o.beta_features = True
                start = time.time()
                print "Start time is: ", time.time()
                #noPoll -> False when GBM finished
                GBMResult = h2o_cmd.runGBM(parseResult=parseResult, noPoll=True,timeoutSecs=timeoutSecs,**kwargs)
                h2j.pollWaitJobs(pattern="GBMKEY",timeoutSecs=1800,pollTimeoutSecs=1800)
                print "Finished time is: ", time.time()
                elapsed = time.time() - start
                print "GBM training completed in", elapsed, "seconds. On dataset: ", csvFilename
                #GBMView = h2o_cmd.runGBMView(model_key='GBMKEY')
                #print GBMView['gbm_model']['errs']

if __name__ == '__main__':
    h2o.unit_main()
