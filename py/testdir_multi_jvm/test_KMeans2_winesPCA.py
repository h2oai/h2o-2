import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_kmeans, h2o_browse as h2b, h2o_import as h2i

#uses the wines data from http://archive.ics.uci.edu/ml/datasets/Wine
#PCA performed to collect data into 2 rows.
#3 groups, small & easy

OLD_KMEANS = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans2_winesPCA(self):
        csvPathname = 'winesPCA.csv'
        start = time.time()
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=10)
        print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o.check_sandbox_for_errors()
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        kwargs = {
            'initialization': 'Furthest',
            # 'initialization': '',
            # 'initialization': 'PlusPlus',
            'max_iter': 50,
            'k': 3,
            'seed': '265211114317615310',
        }

        timeoutSecs = 480

        # try the same thing 5 times
        for trial in range (10):
            start = time.time()

            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "kmeans #", trial, "end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
            (centers, tupleResultList) = \
                h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

            # tupleResultList has tuples = center, rows_per_cluster, sqr_error_per_cluster

            # now compare expected vs actual. By sorting on center, we should be able to compare
            # since the centers should be separated enough to have the order be consistent
            if OLD_KMEANS:
                expected = [
                    ([-2.25977535371875, -0.8631572635625001], 64, 83.77800617624794),
                    ([0.16232721958461543, 1.7626161107230771], 65, 111.64440134649745),
                    ([2.7362112930204074, -1.2107751495102044], 49, 62.6290553489474),
                    ]
            else:
                # error:  258.051462872
                expected = [
                    ([-2.23406681758209, -0.7729819755373136], 67, 96.85372611195429),
                    ([0.25174392601612905, 1.792222172419355], 62, 99.21823733913352),
                    ([2.7362112930204074, -1.2107751495102044], 49, 62.6290553489474),
                        ]

            # multipliers on the expected values for allowed
            # within 2% of best with random seeds?
            allowedDelta = (0.01, 0.01, 0.01)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial)
	    

if __name__ == '__main__':
    h2o.unit_main()
