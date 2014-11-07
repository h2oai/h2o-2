import unittest
import random, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_import as h2i

# RF train parameters
paramsTrainRF = { 
            'ntree'      : 100, 
            'depth'      : 300,
            'bin_limit'  : 20000,
            'ignore'     : None,
            'stat_type'  : 'ENTROPY',
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit': 0,
            'timeoutSecs': 14800,
            }

# RF test parameters
paramsTestRF = {
            # scoring requires the response_variable. it defaults to last, so normally
            # we don't need to specify. But put this here and (above if used) 
            # in case a dataset doesn't use last col 
            'response_variable': None,
            'out_of_bag_error_estimate': 0, 
            'timeoutSecs': 14800,
        }

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_iris(self):
        # Train RF
        trainParseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', hex_key='train_iris2.hex', schema='put')
        kwargs = paramsTrainRF.copy()
        trainResult = h2o_rf.trainRF(trainParseResult, **kwargs)

        scoreParseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', hex_key='score_iris2.hex', schema='put')
        kwargs = paramsTestRF.copy()
        scoreResult  = h2o_rf.scoreRF(scoreParseResult, trainResult, **kwargs)

        print "\nTrain\n=========={0}".format(h2o_rf.pp_rf_result(trainResult))
        print "\nScoring\n========={0}".format(h2o_rf.pp_rf_result(scoreResult))

if __name__ == '__main__':
    h2o.unit_main()
