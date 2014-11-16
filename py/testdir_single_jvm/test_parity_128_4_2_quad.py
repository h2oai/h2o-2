import sys
sys.path.extend(['.','..','../..','py'])

import unittest, h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parity_128_4_2_quad(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='parity_128_4_2_quad.data', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=15)

if __name__ == '__main__':
    h2o.unit_main()
