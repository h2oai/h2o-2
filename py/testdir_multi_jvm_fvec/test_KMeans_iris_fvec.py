import unittest, time, sys, random, math
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_iris_fvec(self):
        h2o.beta_features = True
        csvFilename = 'iris.csv'
        csvPathname = 'iris/' + csvFilename

        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', hex_key=csvFilename + ".hex")

        for trial in range(10):
            # reuse the same seed, to get deterministic results (otherwise sometimes fails
            kwargs = {
                'ignored_cols': 'C5', # ignore the output
                # 'normalize': 0,
                'k': 3, 
                'max_iter': 25,
                'initialization': 'Furthest',
                'destination_key': 'iris.hex', 
                # different answer with this seed
                # 'seed': 265211114317615310,
                'seed': 0,
                }

            timeoutSecs = 90
            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            (centers, tupleResultList)  = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

            expected = [
                # if ignored_cols isn't used
                # ([5, 3.4, 1.46, 0.244, 0.0], 50, 15.24) ,
                # ([5.9, 2.76, 4.26, 1.33, 1.02], 51, 32.9) ,
                # ([6.6, 2.98, 5.57, 2.03, 2.0], 49, 39.15) ,
                ([5.005999999999999, 3.4180000000000006, 1.464, 0.2439999999999999], 50, 15.240400000000003) ,
                ([5.901612903225807, 2.748387096774194, 4.393548387096775, 1.4338709677419357], 62, 39.82096774193549) ,
                ([6.8500000000000005, 3.073684210526315, 5.742105263157894, 2.0710526315789473], 38, 23.87947368421053) ,
            ]
            

            # all are multipliers of expected tuple value
            allowedDelta = (0.01, 0.01, 0.01) 
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)

if __name__ == '__main__':
    h2o.unit_main()
