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

    def test_stedo_testing_data(self):
        csvPathname = h2o.find_file('smalldata/stego/stego_testing.data')
        # Prediction class is the second column => class=1
        h2o_cmd.runRF(trees=50, timeoutSecs=30, csvPathname=csvPathname, response_variable=1)

if __name__ == '__main__':
    h2o.unit_main() 
