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
        # I'll set use_hdfs to False here, because H2O won't start if it can't talk to the hdfs
        # h2o_hosts.build_cloud_with_hosts(use_hdfs=False)
        h2o_hosts.build_cloud_with_hosts(use_hdfs=False,base_port=54321)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_1m_rf_w_browser(self):

        h2b.browseTheCloud()
        csvPathname = h2o.find_file('/smalldata/poker/poker1000')

        # h2o_cmd.runRF(trees=10000, timeoutSecs=300, csvPathname=csvPathname)
        # h2b.browseJsonHistoryAsUrlLastMatch("RFView")
        # browseJsonHistoryAsUrl()

        # hang for many hour, so you can play with the browser
        # FIX!, should be able to do something that waits till browser is quit?
        if not h2o.browse_disable:
            time.sleep(500000)

if __name__ == '__main__':

    h2o.unit_main()
