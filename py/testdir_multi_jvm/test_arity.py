import unittest, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=2)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_arit_rf(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='test/arit.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=20, timeoutSecs=300)

if __name__ == '__main__':
    h2o.unit_main()
