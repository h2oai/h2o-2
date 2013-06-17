# this lets me be lazy..starts the cloud up like I want from my json, and gives me a browser
# copies the jars for me, etc. Just hangs at the end for 10 minutes while I play with the browser
import unittest
import time,sys
sys.path.extend(['.','..','py'])

import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()
    @classmethod
    def setUpClass(cls):
        # Uses your username specific json: pytest_config-<username>.json

        # do what my json says, but with my hdfs. hdfs_name_node from the json
        h2o_hosts.build_cloud_with_hosts(use_hdfs=True,base_port=54321)
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_1(self):
        h2b.browseTheCloud()
        while 1:
          time.sleep(500000)
          print '.'
if __name__ == '__main__':
    h2o.unit_main()
