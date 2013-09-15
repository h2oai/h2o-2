import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

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

        # h2b.browseTheCloud()
        parseResult = h2i.import_parse(bucket='smalldata', path=csvFilename, hex_key=csvFilename + ".hex", schema='put')

        # loop, to see if we get same centers
        # should check the means?
        # both of these centers match what different R/Scikit packages get
        expected1 = [
                # expected centers are from R. rest is just from h2o
                ([310527.2, 13433.89], 11340, None),
                ([5647967.1, 40487.76], 550, None),
                ([21765291.7, 93129.26], 14,  None),
            ]

        # this is what we get with Furthest
        expected2 = [
                ([351104.74065255735, 15421.749823633158], 11340, 5021682274541967.0) ,
                ([7292636.589090909, 7575.630909090909], 550, 6373072701775582.0) ,
                ([34406781.071428575, 244878.0], 14, 123310713697348.92) ,
            ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.0001, 0.0001, 0.0001)
        for trial in range(2):
            kwargs = {
                'k': 3, 
                'max_iter': 50,
                'epsilon': 1e-4,
                'normalize': 0,
                'cols': '0,1',
                'initialization': 'Furthest', 
                # 'initialization': 'PlusPlus',
                'destination_key': 'kmeans_dest_key',
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310
            }

            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvFilename, parseResult, 'd', **kwargs)

            if 1==0:
                h2b.browseJsonHistoryAsUrlLastMatch("KMeansScore")
                h2b.browseJsonHistoryAsUrlLastMatch("KMeansApply")
                h2b.browseJsonHistoryAsUrlLastMatch("KMeans")
                # Comment sleep out to get a clean grep.
                # time.sleep(3600)

            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected2, allowedDelta, trial=trial)



if __name__ == '__main__':
    h2o.unit_main()
