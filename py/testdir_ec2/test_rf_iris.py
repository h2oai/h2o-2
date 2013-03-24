import unittest
import random, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf, h2o_hosts

# RF train parameters
paramsTrainRF = { 
            'ntree'      : 100, 
            'depth'      : 300,
            'parallel'   : 1, 
            'bin_limit'  : 20000,
            'ignore'     : None,
            'gini'       : 0,
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit': 0,
            'timeoutSecs': 14800,
            }

# RF test parameters
paramsTestRF = {
            'timeoutSecs': 14800,
        }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def loadTrainData(self):
        trainFile   = h2o.find_file('smalldata/iris/iris2.csv')
        trainKey    = h2o_cmd.parseFile(csvPathname=trainFile)

        return trainKey
    
    def loadScoreData(self):
        scoreFile   = h2o.find_file('smalldata/iris/iris2.csv')
        scoreKey    = h2o_cmd.parseFile(csvPathname=scoreFile)

        return scoreKey

    def test_rf_iris(self):
        # Train RF
        trainParseKey = self.loadTrainData()
        kwargs = paramsTrainRF.copy()
        trainResult = h2o_rf.trainRF(trainParseKey, **kwargs)

        scoreParseKey = self.loadScoreData()
        kwargs = paramsTestRF.copy()
        scoreResult  = h2o_rf.scoreRF(scoreParseKey, trainResult, **kwargs)

        print "\nTrain\n=========={0}".format(h2o_rf.pp_rf_result(trainResult))
        print "\nScoring\n========={0}".format(h2o_rf.pp_rf_result(scoreResult))

if __name__ == '__main__':
    h2o.unit_main()
