import os, json, unittest, time, shutil, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','py'])
import h2o_cmd
import h2o
import h2o_browse as h2b

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

    def test_fail1_100x1100(self):
        csvPathname=h2o.find_file('smalldata/fail1_100x11000.csv.gz')
        parseKey = h2o_cmd.parseFile(None, csvPathname, timeoutSecs=10, retryDelaySecs=0.15)

if __name__ == '__main__':
    h2o.unit_main()
