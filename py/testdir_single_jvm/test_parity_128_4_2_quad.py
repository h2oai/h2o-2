import sys
sys.path.extend(['.','..','py'])

import unittest, h2o, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parity_128_4_2_quad(self):
        h2o_cmd.runRF(None, h2o.find_file('smalldata/parity_128_4_2_quad.data'), trees=6, 
            timeoutSecs=5)

if __name__ == '__main__':
    h2o.unit_main()
