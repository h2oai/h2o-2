import unittest, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(node_count=2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_arity_rf_fvec(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='test/arit.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=10, timeoutSecs=900)

if __name__ == '__main__':
    h2o.unit_main()
