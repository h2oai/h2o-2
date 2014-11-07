import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_hhp_2_fvec(self):
        # NAs cause CM to zero..don't run for now
        csvPathname = 'hhp_9_17_12.predict.data.gz'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=30)
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=300)

if __name__ == '__main__':
    h2o.unit_main()
