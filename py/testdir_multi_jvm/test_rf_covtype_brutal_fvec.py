import unittest, time, sys, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_util, h2o_import as h2i

USE_LOCAL = True

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
        h2o.init(2, java_heap_GB=7)
        
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
        paramsTrainRF = { 
            'seed': '1234567890',
            # if I use 100, and just one tree, I should get same results for sorted/shuffled?
            # i.e. the bagging always sees everything. Means oobe will be messed up
            # so will specify validation = the 10pct holdout data (could reuse the training data?)
            'sample_rate': 1.0,
            'ntrees': 3, 
            'max_depth': 300,
            'nbins': 200,
            'timeoutSecs': 600,
            'response': 'C55',
        }

        paramsScoreRF = {
            'vactual': 'C55',
            'timeoutSecs': 600,
        }

        # 90% data
        trainKey1 = self.loadData(trainDS1)
        scoreKey1 = self.loadData(scoreDS1)
        kwargs   = paramsTrainRF.copy()
        trainResult1 = h2o_rf.trainRF(trainKey1, scoreKey1, **kwargs)
        (classification_error1, classErrorPctList1, totalScores1) = h2o_rf.simpleCheckRFView(rfv=trainResult1)
        # self.assertEqual(4.29, classification_error1)
        # self.assertEqual([4.17, 2.98, 4.09, 14.91, 21.12, 15.38, 5.22], classErrorPctList1)
        # with new RNG 9/26/14
        self.assertEqual(4.4, classification_error1)
        self.assertEqual([3.71, 3.56, 4.32, 18.55, 21.22, 13.51, 5.82], classErrorPctList1)
        self.assertEqual(58101, totalScores1)

        kwargs   = paramsScoreRF.copy()
        scoreResult1 = h2o_rf.scoreRF(scoreKey1, trainResult1, **kwargs)

        # 10% data
        trainKey2 = self.loadData(trainDS2)
        scoreKey2 = self.loadData(scoreDS2)
        kwargs   = paramsTrainRF.copy()
        trainResult2 = h2o_rf.trainRF(trainKey2, scoreKey2, **kwargs)
        (classification_error2, classErrorPctList2, totalScores2) = h2o_rf.simpleCheckRFView(rfv=trainResult2)
        # self.assertEqual(4.29, classification_error2)
        # self.assertEqual([4.17, 2.98, 4.09, 14.91, 21.12, 15.38, 5.22], classErrorPctList2)
        # with new RNG 9/26/14
        self.assertEqual(4.4, classification_error1)
        self.assertEqual([3.71, 3.56, 4.32, 18.55, 21.22, 13.51, 5.82], classErrorPctList1)
        self.assertEqual(58101, totalScores2)

        kwargs   = paramsScoreRF.copy()
        scoreResult2 = h2o_rf.scoreRF(scoreKey2, trainResult2, **kwargs)

      
        print "\nTraining: JsonDiff sorted data results, to non-sorted results (json responses)"
        df = h2o_util.JsonDiff(trainResult1, trainResult2, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)

        print "\nScoring: JsonDiff sorted data results, to non-sorted results (json responses)"
        df = h2o_util.JsonDiff(scoreResult1, scoreResult2, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)

        # should only be two diffs
        if len(df.difference) > 2:
            raise Exception ("Too many diffs in JsonDiff sorted vs non-sorted %s" % len(df.difference))
        # Scoring: JsonDiff sorted data results, to non-sorted results (json responses)
        # df.difference: [
        # "diff: response_info.time - 28 | 11",
        # "diff: python_call_timer - 0.526123046875 | 0.498980998993"
        # ]


if __name__ == '__main__':
    h2o.unit_main()
