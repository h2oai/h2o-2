# this lets me be lazy..starts the cloud up like I want from my json, and gives me a browser
# copies the jars for me, etc. Just hangs at the end for 10 minutes while I play with the browser
import unittest
import time,sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
import h2o_common

import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()
    @classmethod
    def setUpClass(cls):
        # Uses your username specific json: pytest_config-<username>.json

        # do what my json says, but with my hdfs. hdfs_name_node from the json
        h2o.init(use_hdfs=True)
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_1(self):
        h2b.browseTheCloud()
        csvFilename = "airlines_all.csv"
        csvPathname='airlines/airlines_all.csv'
        hex_key = csvFilename + ".hex"
        start = time.time()
        timeoutSecs=1200
        # airlines_hex = h2i.import_parse(bucket='/home/0xdiag/datasets', path=csvPathname, schema='local', hex_key=hex_key, 
        #             timeoutSecs=timeoutSecs, retryDelaySecs=4, pollTimeoutSecs=60, doSummary=False)
        # print "fv.parse done in ",(time.time()-start)
        # kwargs = {
        #     'ignored_cols':'DepTime,ArrTime,TailNum,ActualElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed',
        #     'standardize': 1,
        #     'classification': 1,
        #     'response': 'IsDepDelayed',
        #     'family': 'binomial',
        #     'n_folds': 0,
        #     'max_iter': 50,
        #     'beta_epsilon': 1e-4,
        #     'lambda':1e-5
        # }
        # results = []
        # for i in range(5):
        #     start = time.time()
        #     glm = h2o_cmd.runGLM(parseResult=airlines_hex, timeoutSecs=timeoutSecs, **kwargs)
        #     auc = glm['glm_model']['submodels'][0]['validation']['auc']
        #     results.append('glm2(%d) done in %d,auc=%f' %(i,(time.time()-start),auc))
        # for s in results:
        #     print s
        while 1:
          time.sleep(500000)
          print '.'
if __name__ == '__main__':
    h2o.unit_main()
