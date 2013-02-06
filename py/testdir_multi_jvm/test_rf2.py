import os, json, unittest, time, shutil, sys
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

    def test_RFhhp(self):
        # NAs cause CM to zero..don't run for now
        ### csvPathnamegz = h2o.find_file('smalldata/hhp_9_17_12.predict.100rows.data.gz')
        csvPathnamegz = h2o.find_file('smalldata/hhp_9_17_12.predict.data.gz')
        h2o_cmd.runRF(trees=6, timeoutSecs=30, csvPathname=csvPathnamegz)

if __name__ == '__main__':
    h2o.unit_main()
