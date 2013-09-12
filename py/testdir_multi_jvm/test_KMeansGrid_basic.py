import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json

import h2o, h2o_cmd, h2o_hosts
import h2o_kmeans, h2o_import2 as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeansGrid_basic(self):
        if localhost:
            csvFilenameList = [
                # ('covtype.data', 60),
                ('covtype.data', 800),
                ]
        else:
            csvFilenameList = [
                ('covtype.data', 800),
                ]

        for csvFilename, timeoutSecs in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path='standard/covtype.data', schema='local',
                timeoutSecs=2000, pollTimeoutSecs=60)
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "python_source:", parseResult['python_source']
            csvPathname = parseResult['python_source']
            
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            params = {
                'k': 2, 
                # 'initialization': 'Furthest', 
                'initialization': None,
                'seed': 3923021996079663354, 
                'normalize': 0, 
                'max_iter': 10
            }
            for trial in range(3):
                kwargs = params.copy()

                start = time.time()
                kmeans = h2o_cmd.runKMeansGrid(parseResult=parseResult, \
                    timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
                elapsed = time.time() - start
                print "kmeans grid end on ", csvPathname, 'took', elapsed, 'seconds.', \
                    "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
                h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

                ### print h2o.dump_json(kmeans)
                inspect = h2o_cmd.runInspect(None,key=kmeans['destination_key'])
                print h2o.dump_json(inspect)

                print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
