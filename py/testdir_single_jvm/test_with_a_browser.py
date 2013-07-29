import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o_cmd, h2o, h2o_hosts
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # Uses your username specific json: pytest_config-<username>.json
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, use_hdfs=True)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_RF_poker_1m_rf_w_browser(self):
        h2b.browseTheCloud()
        # csvPathname = '../smalldata/poker/poker1000'
        # h2o_cmd.runRF(trees=10000, timeoutSecs=300, csvPathname=csvPathname)
        # h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        # browseJsonHistoryAsUrl()

        # hang for many hour, so you can play with the browser
        # FIX!, should be able to do something that waits till browser is quit?
        if not h2o.browse_disable:
            time.sleep(500000)

if __name__ == '__main__':

    h2o.unit_main()
