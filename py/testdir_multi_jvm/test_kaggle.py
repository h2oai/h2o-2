import unittest, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import2 as h2i

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
        ### h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_cs_training(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='kaggle/creditsample-training.csv.gz', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=100, depth=100, timeoutSecs=300, response_variable=1)
        # h2b.browseJsonHistoryAsUrlLastMatch("RFView")

    def test_cs_test(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='kaggle/creditsample-training.csv.gz', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=100, depth=100, timeoutSecs=300, response_variable=1)
        # h2b.browseJsonHistoryAsUrlLastMatch("RFView")

        time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
