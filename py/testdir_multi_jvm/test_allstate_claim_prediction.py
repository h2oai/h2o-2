import unittest, os, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_claim_prediction(self):
        csvPathname = h2o.find_file('smalldata/allstate/claim_prediction_train_set_10000_int.csv.gz')
        h2o_cmd.runRF(trees=50, timeoutSecs=50, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main() 
