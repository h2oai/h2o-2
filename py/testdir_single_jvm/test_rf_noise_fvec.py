import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put', noise=('StoreView',None), retryDelaySecs=0.1, timeoutSecs=30)
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=30, noise=('StoreView',None), retryDelaySecs=0.1)

    def test_C_RF_poker100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker100', schema='put', noise=('StoreView',None), retryDelaySecs=0.1, timeoutSecs=30)
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=30, noise=('StoreView',None), retryDelaySecs=0.1)

    def test_D_GenParity1(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='parity_128_4_100_quad.data', schema='put', noise=('StoreView',None), retryDelaySecs=0.1, timeoutSecs=30)
        h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=30, noise=('StoreView',None), retryDelaySecs=0.1)

    def test_E_ParseManyCols(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='fail1_100x11000.csv.gz', schema='put', noise=('StoreView',None), retryDelaySecs=0.1, timeoutSecs=120)

        print "parseResult:", h2o.dump_json(parseResult)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])

    def test_F_StoreView(self):
        storeView = h2o.nodes[0].store_view()

if __name__ == '__main__':
    h2o.unit_main()
