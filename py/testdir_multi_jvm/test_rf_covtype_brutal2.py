import unittest, time, sys, os
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_rf, h2o_util, h2o_import as h2i

USE_LOCAL=True

# RF train parameters
print "Try params to get best result on build_cloud(1) local (1 jvm) "
print "experimenting with class_weights and strata_samples" 
paramsTrainRF = { 
            # 'class_weights': '1=1,2=2,3=3,4=4,5=5,6=6,7=7',
            # 'class_weights': '1=1,2=1,3=1,4=1,5=1,6=1,7=1',
            'sampling_strategy': 'RANDOM',
            # 'sampling_strategy': 'STRATIFIED_LOCAL',
            # this have to sum to 100%
            # 'strata_samples': '1=14,2=14,3=14,4=14,5=14,6=14,7=14',
            # 'strata_samples': '1=100000,2=100000,3=100000,4=100000,5=100000,6=100000,7=100000',
            # 'strata_samples': '1=99,2=99,3=99,4=99,5=99,6=99,7=99',
            'use_non_local_data' : 1,
            'sample': 66,
            'clear_confusion_matrix' : 1,
            'ntree'      : 10, 
            'depth'      : 300,
            'parallel'   : 1, 
            'bin_limit'  : 20000,
            'stat_type'  : 'ENTROPY',
            # 'stat_type'  : 'GINI',
            'out_of_bag_error_estimate': 1, 
            'exclusive_split_limit'    : 0,
            'timeoutSecs': 300,
            }

# RF test parameters
paramsScoreRF = {
            # scoring requires the response_variable. it defaults to last, so normally
            # we don't need to specify. But put this here and (above if used) 
            # in case a dataset doesn't use last col 
            'response_variable': None,
            'timeoutSecs': 300,
            'out_of_bag_error_estimate': 0, 
            'clear_confusion_matrix' : 1,
        }

trainDS1 = {
        's3bucket'    : 'home-0xdiag-datasets',
        'localbucket' : 'home/0xdiag/datasets',
        'pathname'    : '/standard/covtype.shuffled.90pct.sorted.data',
        'timeoutSecs' : 300,
        'header'      : 0
        }

scoreDS1 = {
        's3bucket'    : 'home-0xdiag-datasets',
        'localbucket' : 'home/0xdiag/datasets',
        'pathname'    : '/standard/covtype.shuffled.10pct.sorted.data',
        'timeoutSecs' : 300,
        'header'      : 0
        }

trainDS2 = {
        's3bucket'    : 'home-0xdiag-datasets',
        'localbucket' : 'home/0xdiag/datasets',
        'pathname'    : '/standard/covtype.shuffled.90pct.data',
        'timeoutSecs' : 300,
        'header'      : 0
        }

scoreDS2 = {
        's3bucket'    : 'home-0xdiag-datasets',
        'localbucket' : 'home/0xdiag/datasets',
        'pathname'    : '/standard/covtype.shuffled.10pct.data',
        'timeoutSecs' : 300,
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
            h2o.build_cloud(3, java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def parseFile(self, s3bucket, localbucket, pathname, timeoutSecs, header, **kwargs):
        if USE_LOCAL: # this can get redirected to s3/s3n by jenkins
            (importFolderPath, csvFilename) = os.path.split("/" + localbucket + pathname)
            h2i.setupImportFolder(None, importFolderPath)
            start = time.time()
            parseResult = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, timeoutSecs=180)

        else:
            schema = "s3n://"
            bucket = s3bucket
            URI = schema + bucket + pathname
            importResult = h2o.nodes[0].import_hdfs(URI)
            start      = time.time()
            parseResult = h2o.nodes[0].parse("*" + pathname, timeoutSecs=timeoutSecs, header=header)

        parse_time = time.time() - start 
        h2o.verboseprint("py-S3 parse took {0} sec".format(parse_time))
        parseResult['python_call_timer'] = parse_time
        return parseResult

    def loadData(self, params):
        kwargs   = params.copy()
        trainKey = self.parseFile(**kwargs)
        return trainKey
    
    def test_RF(self):
        trainKey1 = self.loadData(trainDS1)
        kwargs   = paramsTrainRF.copy()
        trainResult1 = h2o_rf.trainRF(trainKey1, **kwargs)

        scoreKey1 = self.loadData(scoreDS1)
        kwargs   = paramsScoreRF.copy()
        scoreResult1 = h2o_rf.scoreRF(scoreKey1, trainResult1, **kwargs)
        print "\nTrain1\n=========={0}".format(h2o_rf.pp_rf_result(trainResult1))
        print "\nScore1\n========={0}".format(h2o_rf.pp_rf_result(scoreResult1))

        trainKey2 = self.loadData(trainDS2)
        kwargs   = paramsTrainRF.copy()
        trainResult2 = h2o_rf.trainRF(trainKey2, **kwargs)

        scoreKey2 = self.loadData(scoreDS2)
        kwargs   = paramsScoreRF.copy()
        scoreResult2 = h2o_rf.scoreRF(scoreKey2, trainResult2, **kwargs)
        print "\nTrain2\n=========={0}".format(h2o_rf.pp_rf_result(trainResult2))
        print "\nScore2\n========={0}".format(h2o_rf.pp_rf_result(scoreResult2))

        print "\nTraining: JsonDiff sorted data results, to non-sorted results (json responses)"
        df = h2o_util.JsonDiff(trainResult1, trainResult2, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)

        print "\nScoring: JsonDiff sorted data results, to non-sorted results (json responses)"
        df = h2o_util.JsonDiff(scoreResult1, scoreResult2, with_values=True)
        print "df.difference:", h2o.dump_json(df.difference)


if __name__ == '__main__':
    h2o.unit_main()
