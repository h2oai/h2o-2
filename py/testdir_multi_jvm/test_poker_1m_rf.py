import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=2)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_hand_testing_data(self):
        csvPathname = h2o.find_file('smalldata/poker/poker-hand-testing.data')
        h2o_cmd.runRF(trees=45, timeoutSecs=400, retryDelaySecs=5, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main()
