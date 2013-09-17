import unittest, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_common

class releaseTest(h2o_common.SetupOneJVM14, unittest.TestCase):

    def test_SetupOneJVM14(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

if __name__ == '__main__':
    h2o.unit_main()
