import unittest, time, sys, csv
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e, h2o_kmeans, random


USE_BAD_SEED = True

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

    def test_kmeans_bad_iris_seed(self):

        if 1==1:
            outputClasses = 3
            y = 4 # last col
            response = 'response'
            skipSrcOutputHeader = 1
            skipPredictHeader = 1
            trees = 40
            bucket = 'smalldata'
            csvPathname = 'iris/iris2.csv'
            hexKey = 'iris2.csv.hex'
            # Huh...now we apparently need the translate. Used to be:
            # No translate because we're using an Exec to get the data out?, and that loses the encoding?
            #  translate = None
            translate = {'setosa': 0.0, 'versicolor': 1.0, 'virginica': 2.0}
            # one wrong will be 0.66667. I guess with random, that can happen?
            expectedPctWrong = 0.7

        timeoutSecs = 15

        #*****************************************************************************

        parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='put', hex_key=hexKey)
        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
        numCols = inspect["numCols"]
        numRows = inspect["numRows"]

        for trial in range(10):
            seed = random.randint(0, sys.maxint)
            # should pass seed
            # want to ignore the response col? we compare that to predicted

            # if we tell kmeans to ignore a column here, and then use the model on the same dataset to predict
            # does the column get ignored? (this is last col, trickier if first col. (are the centers "right"

            badSeed = 8224855816180210471
            kwargs = {
                'ignored_cols_by_name': response,
                # 'seed': seed,
                'seed': badSeed if USE_BAD_SEED else seed,
                # "seed": 4294494033083512223, 
                'k': outputClasses,
                'initialization': 'PlusPlus',
                'destination_key': 'kmeans_model',
                'max_iter': 1000 }

            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=60, **kwargs)
            # this is what the size of each cluster was, when reported by training
            size = kmeans['model']['size']
            iterations = kmeans['model']['iterations']

            
            expectedMaxIterations = 20
            if iterations > expectedMaxIterations:
                raise Exception("didn't expect more than %s iterations: %s" % (expectedMaxIterations, iterations))

            # tupleResultList is created like this: ( (centers[i], rows_per_cluster[i], sqr_error_per_cluster[i]) )
            # THIS DOES A PREDICT in it (we used to have to do the predict to get more training result info?)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

            # the tupleResultList has the size during predict? compare it to the sizes during training
            # I assume they're in the same order.
            size2 = [t[1] for t in tupleResultList]
            if size!=size2:
                raise Exception("trial: %s training cluster sizes: %s not the same as predict on same data: %s", (trial, size, size2))

            # hack...hardwire for iris here
            # keep this with sizes sorted
            expectedSizes = [
                [39, 50, 61],
                [38, 50, 62],
            ]
            sortedSize = sorted(size)
            if sortedSize not in expectedSizes:
                raise Exception("trial: %s I got cluster sizes %s but expected one of these: %s " % (trial, sortedSize, expectedSizes))
            
            # check center list (first center) has same number of cols as source data
            print "centers:", centers

            # we said to ignore the output so subtract one from expected
            self.assertEqual(numCols-1, len(centers[0]), 
                "trial: %s kmeans first center doesn't have same # of values as dataset row %s %s" % (trial, numCols-1, len(centers[0])))

            # FIX! add expected
            # h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)

            error = kmeans['model']['total_within_SS']
            within_cluster_variances = kmeans['model']['within_cluster_variances']
            print "trial %s within_cluster_variances:", (trial, within_cluster_variances)

if __name__ == '__main__':
    h2o.unit_main()
