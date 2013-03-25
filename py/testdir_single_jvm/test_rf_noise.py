import os, json, unittest, time, shutil, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=3)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_Basic(self):
        h2o.verify_cloud_size()

    def test_B_RF_iris2(self):
        h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/iris/iris2.csv'), noise=('StoreView',None))

    def test_C_RF_poker100(self):
        h2o_cmd.runRF(trees=6, timeoutSecs=10,
                csvPathname = h2o.find_file('smalldata/poker/poker100'), noise=('StoreView',None))

    def test_D_GenParity1(self):
        trees = 50
        h2o_cmd.runRF(trees=50, timeoutSecs=15, 
                csvPathname = h2o.find_file('smalldata/parity_128_4_100_quad.data'), 
                noise=('StoreView', None))

    def test_E_ParseManyCols(self):
        csvPathname=h2o.find_file('smalldata/fail1_100x11000.csv.gz')
        parseKey = h2o_cmd.parseFile(None, csvPathname, timeoutSecs=10, retryDelaySecs=0.15,
            noise=('StoreView', None))
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], offset=-1, view=5)

    def test_F_StoreView(self):
        storeView = h2o.nodes[0].store_view()


if __name__ == '__main__':
    h2o.unit_main()
