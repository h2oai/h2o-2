import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2, java_heap_GB=7)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_poker_1m_fvec(self):
        csvPathname = 'poker/poker-hand-testing.data'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=3, timeoutSecs=800, retryDelaySecs=5)

if __name__ == '__main__':
    h2o.unit_main()
