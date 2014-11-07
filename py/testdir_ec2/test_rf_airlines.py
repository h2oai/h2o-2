import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf

# RF train parameters
paramsTrainRF = { 
            'ntree'      : 50, 
            'depth'      : 30,
            'bin_limit'  : 10000,
            'ignore'     : 'AirTime,ArrDelay,DepDelay,CarrierDelay,IsArrDelayed',
            'stat_type'  : 'ENTROPY',
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit'    : 0,
            'timeoutSecs': 14800,
            'iterative_cm': 0,
            }

# RF test parameters
paramsScoreRF = {
            # scoring requires the response_variable. it defaults to last, so normally
            # we don't need to specify. But put this here and (above if used) 
            # in case a dataset doesn't use last col 
            'response_variable': None,
            'out_of_bag_error_estimate': 0, 
            'timeoutSecs': 14800,
        }

trainDS = {
        's3bucket'    : 'h2o-airlines-unpacked',
        'filename'    : 'allyears1987to2007.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

scoreDS = {
        's3bucket'    : 'h2o-airlines-unpacked',
        'filename'    : 'year2008.csv',
        'timeoutSecs' : 14800,
        'header'      : 1
        }

PARSE_TIMEOUT=14800

class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def parseS3File(self, s3bucket, filename, **kwargs):
        start      = time.time()
        parseResult   = h2o_cmd.parseS3File(bucket=s3bucket, filename=filename, **kwargs)
        parse_time = time.time() - start 
        h2o.verboseprint("py-S3 parse took {0} sec".format(parse_time))
        parseResult['python_call_timer'] = parse_time
        return parseResult

    def loadTrainData(self):
        kwargs   = trainDS.copy()
        trainKey = self.parseS3File(**kwargs)
        return trainKey
    
    def loadScoreData(self):
        kwargs   = scoreDS.copy()
        scoreKey = self.parseS3File(**kwargs)
        return scoreKey 

    def test_RF(self):
        trainKey = self.loadTrainData()
        kwargs   = paramsTrainRF.copy()
        trainResult = h2o_rf.trainRF(trainKey, **kwargs)

        scoreKey = self.loadScoreData()
        kwargs   = paramsScoreRF.copy()
        scoreResult = h2o_rf.scoreRF(scoreKey, trainResult, **kwargs)

        print "\nTrain\n=========={0}".format(h2o_rf.pp_rf_result(trainResult))
        print "\nScoring\n========={0}".format(h2o_rf.pp_rf_result(scoreResult))

if __name__ == '__main__':
    h2o.unit_main()
