import unittest, random, sys, time, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i

def define_params(SEED):
    paramDict = {
        'k': [2, 5], # seems two slow tih 12 clusters if all cols
        'initialization': ['None', 'PlusPlus', 'Furthest'],
        'ignored_cols': [None, "0", "3", "0,1,2,3,4"],
        'seed': [None, 12345678, SEED],
        'normalize': [None, 0, 1],
        'max_iter': [10,20,50],
        # 'destination_key:': "junk",
        
        }
    return paramDict

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
        h2o.tear_down_cloud()

    def test_KMeans_params_rand2_fvec(self):
        if h2o.localhost:
            csvFilenameList = [
                # ('covtype.data', 60),
                ('covtype.data', 800),
                ]
        else:
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

            paramDict = define_params(SEED)
            for trial in range(3):
                # default
                params = {
                    'max_iter': 20, 
                    'k': 1, 
                    'destination_key': csvFilename + "_" + str(trial) + '.hex'
                }
                h2o_kmeans.pickRandKMeansParams(paramDict, params)
                kwargs = params.copy()

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
