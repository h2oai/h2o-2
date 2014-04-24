import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
import h2o_common

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_c9_GLM_airlines_hdfs(self):
        h2o.beta_features = True

        files = [
                 ('datasets', 'airlines_all.csv', 'airlines_all.hex', 1800, 'IsDepDelayed')
                ]

        for importFolderPath, csvFilename, trainKey, timeoutSecs, response in files:
            # PARSE train****************************************
            csvPathname = importFolderPath + "/" + csvFilename
            
            start = time.time()
            parseResult = h2i.import_parse(path=csvPathname, schema='hdfs', hex_key=trainKey, 
                timeoutSecs=timeoutSecs)
            elapsed = time.time() - start
            print "parse end on ", csvFilename, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # GLM (train)****************************************
            params = {
                'lambda': 1e-4,
                'alpha': 0.5,
                'max_iter': 30,
                'n_folds': 3,
                'family': 'binomial',
                'destination_key': "GLMKEY",
                'response': response,
                'ignored_cols_by_name': 'CRSDepTime,CRSArrTime,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed'
            }
            kwargs = params.copy()
            timeoutSecs = 1800
            start = time.time()
            GLMResult = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs,**kwargs)
            elapsed = time.time() - start
            print "GLM training completed in", elapsed, "seconds. On dataset: ", csvFilename

        h2i.delete_keys_at_all_nodes(timeoutSecs=600)


if __name__ == '__main__':
    h2o.unit_main()
