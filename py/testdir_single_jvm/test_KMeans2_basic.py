import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i, h2o_jobs

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()

        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_kmeans_benign(self):
        importFolderPath = "logreg"
        csvFilename = "benign.csv"
        hex_key = "benign.hex"

        csvPathname = importFolderPath + "/" + csvFilename
        # FIX! hex_key isn't working with Parse2 ? parseResult['destination_key'] not right?
        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, header=1, 
            timeoutSecs=180, doSummary=False)

        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename


        expected = [
            ([8.86, 2.43, 35.53, 0.31, 13.22, 1.47, 1.33, 20.06, 13.08, 0.53, 2.12, 128.61, 35.33, 1.57], 49, None), 
            ([33.47, 2.29, 50.92, 0.34, 12.82, 1.33, 1.36, 21.43, 13.30, 0.37, 2.52, 125.40, 43.91, 1.79], 87, None), 
            ([27.64, 2.87, 48.11, 0.09, 11.80, 0.98, 1.51, 21.02, 12.53, 0.58, 2.89, 171.27, 42.73, 1.53], 55, None), 
            ([26.00, 2.67, 46.67, 0.00, 13.00, 1.33, 1.67, 21.56, 11.44, 0.22, 2.89, 234.56, 39.22, 1.56], 9, None), 
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01, 0.01)

        # loop, to see if we get same centers

        for trial in range(1):
            kmeansSeed = random.randint(0, sys.maxint)
            # kmeansSeed = 6655548259421773879

            kwargs = {
                'k': 4, 
                'initialization': 'PlusPlus',
                'destination_key': 'benign_k.hex', 
                # 'seed': 265211114317615310, 
                'max_iter': 50,
                'seed': kmeansSeed,
            }
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
            # h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=0)


    def test_kmeans_prostate(self):

        importFolderPath = "logreg"
        csvFilename = "prostate.csv"
        hex_key = "prostate.hex"
        csvPathname = importFolderPath + "/" + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, header=1, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename

        # loop, to see if we get same centers

        expected = [
            ([0.37,65.77,1.07,2.23,1.11,10.49,4.24,6.31], 215, 36955), 
            ([0.36,66.44,1.09,2.21,1.06,10.84,34.16,6.31], 136, 46045),
            ([0.83,66.17,1.21,2.86,1.34,73.30,15.57,7.31], 29, 33412),
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01)
        for trial in range(1):
            # kmeansSeed = random.randint(0, sys.maxint)
            # actually can get a slightly better error sum with a different seed
            # this seed gets the same result as scikit
            kmeansSeed = 6655548259421773879

            kwargs = {
                'ignored_cols': 'ID',
                'k': 3, 
                # 'initialization': 'Furthest', 
                'initialization': 'PlusPlus',
                'destination_key': 'prostate_k.hex', 
                'max_iter': 500,
                'seed': kmeansSeed,
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                # 'seed': 265211114317615310}
            }

            # for fvec only?
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)





if __name__ == '__main__':
    h2o.unit_main()
