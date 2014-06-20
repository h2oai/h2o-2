import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o_hosts
import h2o, h2o_cmd, h2o_import as h2i

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

    def test_players_NA_fvec(self):
        h2o.beta_features = True
        csvFilename = 'Players.csv'
        csvPathname = 'ncaa/' + csvFilename
        # hack it to ignore header (NA?) because it has duplicate col names
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', 
            timeoutSecs=15, header=0)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        missingValuesList = h2o_cmd.infoFromInspect(inspect, csvPathname)
        print missingValuesList
        # self.assertEqual(missingValuesList, [], "Players.csv should have no NAs")

if __name__ == '__main__':
    h2o.unit_main()
