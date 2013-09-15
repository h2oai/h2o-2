import unittest, time, random, sys
sys.path.extend(['.','..','py','../h2o/py','../../h2o/py'])
import h2o, h2o_common, h2o_cmd, h2o_import as h2i

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_B_RF_iris2(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

if __name__ == '__main__':
    h2o.unit_main()
