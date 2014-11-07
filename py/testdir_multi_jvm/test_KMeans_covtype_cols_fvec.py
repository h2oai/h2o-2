import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_kmeans

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_KMeans_covtype_cols_fvec(self):
        # just do the import folder once
        # make the timeout variable per dataset. it can be 10 secs for covtype 20x (col key creation)
        # so probably 10x that for covtype200
        csvFilenameList = [
            ("covtype.binary.svm", "cC", 30, 1),
            # normal csv
        ]

        ### csvFilenameList = random.sample(csvFilenameAll,1)
        # h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        importFolderPath = "libsvm"
        for (csvFilename, hex_key, timeoutSecs, resultMult) in csvFilenameList:
            # have to import each time, because h2o deletes source after parse
            csvPathname = importFolderPath + "/" + csvFilename

            # PARSE******************************************
            # creates csvFilename.hex from file in importFolder dir 
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, 
                hex_key=hex_key, timeoutSecs=2000)
            print "Parse result['destination_key']:", parseResult['destination_key']

            # INSPECT******************************************
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvFilename)
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            # KMEANS******************************************
            for trial in range(1):
                kwargs = {
                    'k': 3, 
                    'initialization': 'Furthest',
                    'ignored_cols': range(11, numCols),
                    'max_iter': 10,
                    # 'normalize': 0,
                    # reuse the same seed, to get deterministic results (otherwise sometimes fails
                    'seed': 265211114317615310,
                }

                # fails if I put this in kwargs..i.e. source = dest
                # 'destination_key': parseResult['destination_key'],

                for trial2 in range(3):
                    timeoutSecs = 600
                    start = time.time()
                    kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                    elapsed = time.time() - start
                    print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                        "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
                    # this does an inspect of the model and prints the clusters
                    h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

                    (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)



if __name__ == '__main__':
    h2o.unit_main()
