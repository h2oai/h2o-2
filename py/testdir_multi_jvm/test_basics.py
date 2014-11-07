import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b
import h2o_common

class Basic(h2o_common.SetupUnitTest, unittest.TestCase):
    nodes = 2
    java_xmx = 7

    def test_A_Basic(self):
        ### h2o.verify_cloud_size()
        pass

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_C_RF_poker100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker100', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

    def test_D_GenParity1(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='parity_128_4_100_quad.data', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=60)

    def test_E_ParseManyCols(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='fail1_100x11000.csv.gz', schema='put', timeoutSecs=100)
        inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])

    # default now wants all data at each node? too slow?
    def test_F_RF_covtype(self):
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path='standard/covtype.data', schema='put', timeoutSecs=100)
        h2o_cmd.runRF(parseResult=parseResult, trees=1, timeoutSecs=300, retryDelaySecs=0.5)

    def test_G_StoreView(self):
        h2i.delete_keys_at_all_nodes(timeoutSecs=30)

if __name__ == '__main__':
    h2o.unit_main()
