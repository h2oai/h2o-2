import unittest, time, sys, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_rf, h2o_util, h2o_import as h2i

USE_LOCAL=True

# RF test parameters

trainDS1 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.90pct.sorted.data',
    'timeoutSecs' : 60,
    'hex_key'     : 'scoreDS1.hex',
    'header'      : 0
    }

scoreDS1 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.10pct.sorted.data',
    'timeoutSecs' : 60,
    'hex_key'     : 'trainDS1.hex',
    'header'      : 0
    }

trainDS2 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.90pct.data',
    'timeoutSecs' : 60,
    'hex_key'     : 'trainDS2.hex',
    'header'      : 0
    }

scoreDS2 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.10pct.data',
    'timeoutSecs' : 60,
    'hex_key'     : 'scoreDS2.hex',
    'header'      : 0
    }


class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def parseFile(self, bucket, pathname, timeoutSecs, header, hex_key, **kwargs):
        # this can get redirected
        if USE_LOCAL:
            schema = None
        else:
            schema = 's3n'

        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=pathname, schema='put', 
            hex_key=hex_key, timeoutSecs=180, **kwargs)
        parse_time = time.time() - start
        h2o.verboseprint("parse took {0} sec".format(parse_time))
        parseResult['python_call_timer'] = parse_time
        return parseResult

    def loadData(self, params):
        kwargs   = params.copy()
        trainKey = self.parseFile(**kwargs)
        return trainKey
    
    def test_RF(self):
        h2o.beta_features = True

        if h2o.beta_features:
            paramsTrainRF = {
                'ntrees': 3,
                'max_depth': 10,
                'nbins': 50,
                'timeoutSecs': 600,
                'response': 'C55',
                'classification': 1,
            }

            paramsScoreRF = {
                'vactual': 'C55',
                'timeoutSecs': 600,
            }

        else:
            paramsTrainRF = {
                'use_non_local_data' : 1,
                'ntree'      : 10,
                'depth'      : 300,
                'bin_limit'  : 20000,
                'stat_type'  : 'ENTROPY',
                'out_of_bag_error_estimate': 1,
                'exclusive_split_limit'    : 0,
                'timeoutSecs': 60,
            }

            paramsScoreRF = {
                # scoring requires the response_variable. it defaults to last, so normally
                # we don't need to specify. But put this here and (above if used) 
                # in case a dataset doesn't use last col 
                'response_variable': None,
                'timeoutSecs': 60,
                'out_of_bag_error_estimate': 0,
            }


        # train1
        trainKey1 = self.loadData(trainDS1)
        kwargs   = paramsTrainRF.copy()
        trainResult1 = h2o_rf.trainRF(trainKey1, **kwargs)

        scoreKey1 = self.loadData(scoreDS1)
        kwargs   = paramsScoreRF.copy()
        h2o_cmd.runInspect(key='scoreDS1.hex', verbose=True)
        scoreResult1 = h2o_rf.scoreRF(scoreKey1, trainResult1, **kwargs)
        h2o_cmd.runInspect(key='Predict.hex', verbose=True)
        print "\nTrain1\n=========="
        h2o_rf.simpleCheckRFScore(node=None, rfv=trainResult1, noPrint=False, **kwargs)
        print "\nScore1\n=========+"
        print h2o.dump_json(scoreResult1)
        h2o_rf.simpleCheckRFScore(node=None, rfv=scoreResult1, noPrint=False, **kwargs)

        # train2
        trainKey2 = self.loadData(trainDS2)
        kwargs   = paramsTrainRF.copy()
        trainResult2 = h2o_rf.trainRF(trainKey2, **kwargs)

        scoreKey2 = self.loadData(scoreDS2)
        kwargs   = paramsScoreRF.copy()
        h2o_cmd.runInspect(key='scoreDS2.hex', verbose=True)
        scoreResult2 = h2o_rf.scoreRF(scoreKey2, trainResult2, **kwargs)
        h2o_cmd.runInspect(key='Predict.hex', verbose=True)
        print "\nTrain2\n=========="
        h2o_rf.simpleCheckRFScore(node=None, rfv=trainResult2, noPrint=False, **kwargs)
        print "\nScore2\n=========="
        h2o_rf.simpleCheckRFScore(node=None, rfv=scoreResult2, noPrint=False, **kwargs)

        if 1==0:
            print "\nTraining: JsonDiff sorted data results, to non-sorted results (json responses)"
            df = h2o_util.JsonDiff(trainResult1, trainResult2, with_values=True)
            print "df.difference:", h2o.dump_json(df.difference)

            print "\nScoring: JsonDiff sorted data results, to non-sorted results (json responses)"
            df = h2o_util.JsonDiff(scoreResult1, scoreResult2, with_values=True)
            print "df.difference:", h2o.dump_json(df.difference)

if __name__ == '__main__':
    h2o.unit_main()
