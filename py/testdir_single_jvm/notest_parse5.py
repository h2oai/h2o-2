import unittest
import re, os, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts

# test some random csv data, and some lineend combinations

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

    # believe the interesting thing is the NaN in the csv
    def test_A_parse5(self):
        csvPathname = h2o.find_file('smalldata/parse5.csv')
        h2o_cmd.runRF(trees=37, timeoutSecs=10, csvPathname=csvPathname)


if __name__ == '__main__':
    h2o.unit_main()
