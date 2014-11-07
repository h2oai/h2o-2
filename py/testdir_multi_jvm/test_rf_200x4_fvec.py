import unittest, time, sys, os
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

    def test_rf_200x4_fvec(self):
        csvPathname = 'hhp.cut3.214.data.gz'

        print "RF start on ", csvPathname
        start = time.time()
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, ntrees=3,
                timeoutSecs=1200, retryDelaySecs=15)
        print "RF end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
