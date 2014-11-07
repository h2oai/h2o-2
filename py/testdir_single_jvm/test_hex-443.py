import unittest, sys
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hex_443(self):
        csvPathname = 'hex-443.parsetmp_1_0_0_0.data'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, ntrees=1, timeoutSecs=5)

if __name__ == '__main__':
    h2o.unit_main()
