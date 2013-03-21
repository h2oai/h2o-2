import unittest, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b
import time

class TestKaggle(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2)
        else:
            h2o_hosts.build_cloud_with_hosts()

        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_cs_training(self):
        h2o_cmd.runRF(trees=100, depth=100, csvPathname=h2o.find_file('smalldata/kaggle/creditsample-training.csv.gz'),timeoutSecs=300, response_variable=1)
        h2b.browseJsonHistoryAsUrlLastMatch("RFView")

    def test_cs_test(self):
        h2o_cmd.runRF(trees=100, depth=100, csvPathname=h2o.find_file('smalldata/kaggle/creditsample-training.csv.gz'),timeoutSecs=300, response_variable=1)

        h2b.browseJsonHistoryAsUrlLastMatch("RFView")
        time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
