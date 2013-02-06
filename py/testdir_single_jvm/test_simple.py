import unittest, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_1m_rf(self):
        csvPathname = h2o.find_file('smalldata/poker/poker1000')
        h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main()
