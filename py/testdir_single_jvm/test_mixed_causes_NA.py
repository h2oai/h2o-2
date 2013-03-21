import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_mixed_causes_NA(self):
        csvFilename = 'mixed_causes_NA.csv'
        csvPathname = h2o.find_file('smalldata/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
        sum_num_missing_values = h2o_cmd.info_from_inspect(inspect, csvPathname)
        self.assertEqual(sum_num_missing_values, 0,
                "Single column of data with mixed number/string should not have NAs")

if __name__ == '__main__':
    h2o.unit_main()
