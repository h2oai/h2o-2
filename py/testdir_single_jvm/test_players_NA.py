import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud()
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_players_NA(self):
        csvFilename = 'Players.csv'
        csvPathname = h2o.find_file('/home/0xdiag/datasets/ncaa/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
        sum_num_missing_values = h2o_cmd.info_from_inspect(inspect, csvPathname)
        self.assertEqual(sum_num_missing_values, 0,
                "Players.csv should not have NAs (although it might have some rows with extras)")

if __name__ == '__main__':
    h2o.unit_main()
