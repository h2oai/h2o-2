import unittest, random, sys, time, json
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_kmeans, h2o_import as h2i

def define_params(SEED):
    paramDict = {
        'k': [2, 5], # seems two slow tih 12 clusters if all cols
        'initialization': ['None', 'PlusPlus', 'Furthest'],
        'cols': [None, "0", "3", "0,1,2,3,4,5,6"],
        'max_iter': [1, 5, 10, 20],
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
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_params_rand2(self):
        if localhost:
            csvFilenameList = [
                # ('covtype.data', 60),
                ('covtype20x.data', 800),
                ]
        else:
            csvFilenameList = [
                ('covtype20x.data', 800),
                ]

        importFolderPath = "standard"
        for csvFilename, timeoutSecs in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname,
                timeoutSecs=2000, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            paramDict = define_params(SEED)
            for trial in range(3):
                # default
                params = {'k': 1, 'destination_key': csvFilename + "_" + str(trial) + '.hex'}

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
                inspect = h2o_cmd.runInspect(None,key=kmeans['destination_key'])
                print h2o.dump_json(inspect)

                print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
