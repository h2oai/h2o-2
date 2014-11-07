import unittest, random, sys, time, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd
import h2o_kmeans, h2o_import as h2i, h2o_jobs as h2j

def define_params(SEED):
    paramDict = {
        # always do grid (see default below)..no destination key should be specified if grid?
        # comma separated or range from:to:step
        'k': ['2,3,4', '2,4'],
        'initialization': ['None', 'PlusPlus', 'Furthest'],
        # not used in Grid?
        # 'cols': [None, "0", "3", "0,1,2,3,4,5,6"],
        'max_iter': [1, 5, 10, 20], # FIX! comma separated or range from:to:step.
        'seed': [None, 12345678, SEED],
        'normalize': [None, 0, 1],
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
        h2o.init(2,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeansGrid_params_rand2_fvec(self):
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
                destinationKey = csvFilename + "_" + str(trial) + '.hex'
                params = {'k': '2,3', 'destination_key': destinationKey}

                h2o_kmeans.pickRandKMeansParams(paramDict, params)
                kwargs = params.copy()
        
                start = time.time()
                kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
                    timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, noPoll=True, **kwargs)
                h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)

                elapsed = time.time() - start
                print "FIX! how do we get results..need redirect_url"
                print "Have to inspect different models? (grid)"
                print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                    "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
                # h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

                ### print h2o.dump_json(kmeans)
                # destination_key is ignored by kmeans...what are the keys for the results
                # inspect = h2o_cmd.runInspect(None,key=destinationKey)
                # print h2o.dump_json(inspect)

                print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
