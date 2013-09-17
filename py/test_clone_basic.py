#!/usr/bin/python
# some typical imports. Also the sys.path is extended since we're in h2o-perf, next to h2o
import unittest, time, sys, random
sys.path.extend(['.','..','py','../h2o/py','../../h2o/py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_kmeans

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # standard method for being able to reproduce the random.* seed
        h2o.setup_random_seed()
        h2o.build_cloud_with_json()

    @classmethod
    def tearDownClass(cls):
        # DON"T
        ### h2o.tear_down_cloud()

        # Instead: All tests should delete their keys..i.e. leave things clean for the next test
        start = time.time()
        h2i.delete_keys_at_all_nodes()
        elapsed = time.time() - start
        print "delete_keys_at_all_nodes(): took", elapsed, "secs"

    def test_clone_basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_C_RF_poker100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker100', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_D_GenParity1(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='parity_128_4_100_quad.data', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=15)

    def test_E_ParseManyCols(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='fail1_100x11000.csv.gz', schema='put', timeoutSecs=10)
        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])

    def test_F_RF_covtype(self):
        parseResult = h2i.import_parse(bucket='datasets', path='UCI/UCI-large/covtype/covtype.data', schema='put', timeoutSecs=30)
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=35, retryDelaySecs=0.5)

if __name__ == '__main__':
    h2o.unit_main()
