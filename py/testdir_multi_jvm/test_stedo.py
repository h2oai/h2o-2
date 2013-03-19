import unittest, os, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_stedo_testing_data(self):
        csvPathname = h2o.find_file('smalldata/stego/stego_training.data')
        # Prediction class is the second column => class=1
        h2o_cmd.runRF(trees=50, timeoutSecs=30, csvPathname=csvPathname, response_variable=1, out_of_bag_error_estimate=1)

if __name__ == '__main__':
    h2o.unit_main() 
