import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_import as h2i, h2o_jobs, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def notest_B_kmeans_benign(self):
        h2o.beta_features = True
        csvPathname = "logreg"
        csvFilename = "benign.csv"
        hex_key = csvFilename + ".hex"
        print "\nStarting", csvFilename
        
        parseResult = h2i.import_parse(bucket='smalldata', 
            path=csvPathname + "/"+csvFilename, schema='local', hex_key=hex_key)

        # FIX! have to fill in expected rows and error here
        # this is from sklearn.cluster.KMeans, with NA's converted to 0
        expected = [
            ([ 8.86,  2.43, 35.53,  0.31, 13.22,  1.47,  1.33, 20.06, 13.08,  0.53,  2.12, 128.61, 35.33,  1.57], 0, 0),
            ([33.47,  2.29, 50.92,  0.34, 12.82,  1.33,  1.36, 21.43, 13.30,  0.37,  2.52, 125.40, 43.91,  1.79], 0, 0),
            ([27.64,  2.87, 48.11,  0.09, 11.80,  0.98,  1.51, 21.02, 12.53,  0.58,  2.89, 171.27, 42.73,  1.53], 0, 0),
            ([26.00,  2.67, 46.67,  0.00, 13.00,  1.33,  1.67, 21.56, 11.44,  0.22,  2.89, 234.56, 39.22,  1.56], 0, 0),
            ]

        
        for i in range(14):
            execExpr = '%s[,%s] = is.na(%s[,%s]) ? 0.0 : %s[,%s]' % (hex_key,i+1,hex_key,i+1,hex_key,i+1)
            h2e.exec_expr(execExpr=execExpr, resultKey=None, timeoutSecs=4)


        # all are multipliers of expected tuple value
        allowedDelta = (0.1, 0.1, 0.1)

        # loop, to see if we get same centers
        for trial in range(2):
            params = {'k': 4, 
                      # 'initialization': 'Furthest', 
                      'initialization': 'PlusPlus', 
                      'destination_key': 'benign_k.hex',
                      'max_iter': 100,
                      # 'seed': 265211114317615310,
                     }
            kwargs = params.copy()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvFilename, parseResult, 'd', **kwargs)
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)


    def test_C_kmeans_prostate(self):
        h2o.beta_features = True
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', 
            path='logreg/'+csvFilename, schema='local', hex_key=csvFilename+".hex")

        # loop, to see if we get same centers
        # this was sklearn.cluster.Kmeans with first col removed. num_rows and error is 0 here 
        expected = [
            ([0.37, 65.77,  1.07,  2.23,  1.11, 10.49,  4.24,  6.31], 0, 0), 
            ([0.36, 66.44,  1.09,  2.21,  1.06, 10.84, 34.16,  6.31], 0, 0),
            ([0.83, 66.17,  1.21,  2.86,  1.34, 73.30, 15.57,  7.31], 0, 0),
            ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.1, 0.1, 0.1)
        for trial in range(2):
            if h2o.beta_features:
                params = {'k': 3, 
                         # 'initialization': 'Furthest', 
                         'initialization': 'PlusPlus',
                         'ignored_cols': "ID",
                         'destination_key': 'prostate_k.hex',
                         'max_iter': 100,
                         'seed': 265211114317615310
                        }
            else:
                params = {'k': 3, 
                         # 'initialization': 'Furthest', 
                         'initialization': 'PlusPlus',
                         'cols': 'CAPSULE, AGE, RACE, DPROS, DCAPS, PSA, VOL, GLEASON',
                         'destination_key': 'prostate_k.hex',
                         'max_iter': 100,
                         'seed': 265211114317615310
                        }

            kwargs = params.copy()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvFilename, parseResult, 'd', **kwargs)
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)

if __name__ == '__main__':
    h2o.unit_main()
