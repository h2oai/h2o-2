import unittest, time, sys, random, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_iris_fvec(self):
        csvFilename = 'iris.csv'
        csvPathname = 'iris/' + csvFilename

        print "\nStarting", csvFilename
        hex_key = csvFilename + ".hex"
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', hex_key=hex_key)

        k = 3
        ignored_cols = 'C5'
        for trial in range(3):
            # reuse the same seed, to get deterministic results (otherwise sometimes fails
            kwargs = {
                'ignored_cols': ignored_cols, # ignore the output
                'k': k, 
                'max_iter': 25,
                'initialization': 'Furthest',
                'destination_key': 'iris.hex', 
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

            gs = h2o.nodes[0].gap_statistic(source=hex_key, ignored_cols=ignored_cols, k_max=k+1)
            print "gap_statistic:", h2o.dump_json(gs)

            k_best = gs['gap_model']['k_best']
            self.assertTrue(k_best!=0, msg="k_best shouldn't be 0: %s" % k_best)

if __name__ == '__main__':
    h2o.unit_main()
