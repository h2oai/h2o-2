import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json

import h2o, h2o_cmd
import h2o_kmeans, h2o_import as h2i

def define_params():
    paramDict = {
        'k': [1, 12],
        'y': [None, 7, 43, 54],
        'epsilon': [1e-8, 1e-6, 1e-2, 1, 10],
        'cols': [None, "0,1,2,3,4,5,6"],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_params_rand2(self):
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED =
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        if localhost:
            csvFilenameList = [
                # ('covtype.data', 60),
                ('covtype20x.data', 400),
                ]
        else:
            csvFilenameList = [
                ('covtype20x.data', 400),
                ('covtype200x.data', 2000),
                ]

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        for csvFilename, timeoutSecs in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath,
                timeoutSecs=2000, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            csvPathname = importFolderPath + "/" + csvFilename
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            paramDict = define_params()
            for trial in range(3):
                randomV = paramDict['k']
                k = random.choice(randomV)

                randomV = paramDict['y']
                y = random.choice(randomV)

                randomV = paramDict['epsilon']
                epsilon = random.choice(randomV)

                randomV = paramDict['cols']
                cols = random.choice(randomV)

                kwargs = {'k': k, 'epsilon': epsilon, 'cols': cols, 'destination_key': csvFilename + "_" + str(trial) + '.hex'}
                start = time.time()
                kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, \
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
