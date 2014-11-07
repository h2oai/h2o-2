import unittest, os, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_stedo_fvec(self):
        csvPathname = 'stego/stego_training.data'
        # Prediction class is the second column => class=1
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, ntrees=10, timeoutSecs=300, response=1)

if __name__ == '__main__':
    h2o.unit_main() 
