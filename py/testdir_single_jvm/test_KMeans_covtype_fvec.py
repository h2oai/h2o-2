import unittest, random, sys, time, json
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_kmeans, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_covtype_fvec(self):
        h2o.beta_features = True
        csvFilenameList = [
            ('covtype.data', 800),
            ]

        importFolderPath = "standard"
        for csvFilename, timeoutSecs in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname,
                timeoutSecs=2000, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            for trial in range(3):
                kwargs = {
                    'k': 3,
                    'initialization': 'Furthest',
                    'ignored_cols': range(11, inspect['numCols']),
                    'max_iter': 10,
                    # 'normalize': 0,
                    # reuse the same seed, to get deterministic results
                    'seed': 265211114317615310
                }

                start = time.time()
                kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
                    timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
                elapsed = time.time() - start
                print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                    "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
                h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
                ### print h2o.dump_json(kmeans)

                print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
