import unittest, time, sys, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_rf, h2o_util, h2o_import as h2i

USE_LOCAL=True

# RF train parameters


# RF test parameters

trainDS1 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.90pct.sorted.data',
    'timeoutSecs' : 60,
    'header'      : 0
    }

scoreDS1 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.10pct.sorted.data',
    'timeoutSecs' : 60,
    'header'      : 0
    }

trainDS2 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.90pct.data',
    'timeoutSecs' : 60,
    'header'      : 0
    }

scoreDS2 = {
    'bucket'      : 'home-0xdiag-datasets',
    'pathname'    : 'standard/covtype.shuffled.10pct.data',
    'timeoutSecs' : 60,
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
            h2o.build_cloud(2)
        else:
            h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def parseFile(self, bucket, pathname, timeoutSecs, header, **kwargs):
        # this can get redirected
        if USE_LOCAL: 
            schema = None
        else:
            schema = 's3n'

        start = time.time()
        parseResult = h2i.import_parse(bucket=bucket, path=pathname, schema='local', timeoutSecs=180)
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
                'ntrees': 10, 
                'max_depth': 300,
                'nbins': 200,
                'timeoutSecs': 600,
                'response': 'C55',
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

        trainKey1 = self.loadData(trainDS1)
        kwargs   = paramsTrainRF.copy()
        trainResult1 = h2o_rf.trainRF(trainKey1, **kwargs)

        scoreKey1 = self.loadData(scoreDS1)
        kwargs   = paramsScoreRF.copy()
        scoreResult1 = h2o_rf.scoreRF(scoreKey1, trainResult1, **kwargs)

        trainKey2 = self.loadData(trainDS2)
        kwargs   = paramsTrainRF.copy()
        trainResult2 = h2o_rf.trainRF(trainKey2, **kwargs)

        scoreKey2 = self.loadData(scoreDS2)
        kwargs   = paramsScoreRF.copy()
        scoreResult2 = h2o_rf.scoreRF(scoreKey2, trainResult2, **kwargs)

        print "\nTraining: JsonDiff sorted data results, to non-sorted results (json responses)"
        df = h2o_util.JsonDiff(trainResult1, trainResult2, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)

        print "\nScoring: JsonDiff sorted data results, to non-sorted results (json responses)"
        df = h2o_util.JsonDiff(scoreResult1, scoreResult2, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)

if __name__ == '__main__':
    h2o.unit_main()
