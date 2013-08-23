import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_twit(self):
        csvFilename = "Twitter2DB.txt"
        print "\nStarting", csvFilename
        csvPathname = h2o.find_file('smalldata/' + csvFilename)

        h2b.browseTheCloud()
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        # loop, to see if we get same centers
        # should check the means?
        # FIX! have to fix these to right answers
        expected = [
                ([5647967.1, 40487.76], 1000000,   60028168),
                ([21765291.7, 93129.26], 2000000,  479913618),
                ([310527.2, 13433.89], 3000000, 1619244994),
            ]
        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01)
        for trial in range(2):
            kwargs = {
                'k': 3, 
                'max_iter': 50,
                'epsilon': 1e-4,
                'normalize': 0,
                'cols': '0,1',
                'initialization': 'Furthest', 
                'destination_key': parseKey['destination_key'],
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310
            }

            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseKey, 'd', **kwargs)

            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)


if __name__ == '__main__':
    h2o.unit_main()
