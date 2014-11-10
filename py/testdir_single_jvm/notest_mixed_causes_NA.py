import unittest, random, sys, time
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

    def test_NOPASS_mixed_causes_NA(self):
        csvFilename = 'mixed_causes_NA.csv'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvFilename, timeoutSecs=15, schema='put')
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        missingValuesList = h2o_cmd.infoFromInspect(inspect, csvFilename)
        print missingValuesList
        self.assertEqual(sum(missingValuesList), 0,
                "Single column of data with mixed number/string should not have NAs")

if __name__ == '__main__':
    h2o.unit_main()
