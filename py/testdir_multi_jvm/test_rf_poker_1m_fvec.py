import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2, java_heap_GB=7)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_poker_1m_fvec(self):
        h2o.beta_features = True
        csvPathname = 'poker/poker-hand-testing.data'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=15, timeoutSecs=800, retryDelaySecs=5)

if __name__ == '__main__':
    h2o.unit_main()
