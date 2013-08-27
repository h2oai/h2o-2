import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts(1, 
                # this is for our amazon ec hdfs
                # see https://github.com/0xdata/h2o/wiki/H2O-and-s3n
                hdfs_name_node='10.78.14.235:9000',
                hdfs_version='0.20.2')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_mnist_s3n(self):
        URI = "s3n://home-0xdiag-datasets/mnist/"
        csvFilelist = [
            ("mnist_training.csv.gz", "mnist_testing.csv.gz",    600), 
            ("mnist_testing.csv.gz",  "mnist_training.csv.gz",    600), 
            ("mnist_training.csv.gz", "mnist_training.csv.gz",    600), 
        ]
        # IMPORT**********************************************
        importHDFSResult = h2o.nodes[0].import_hdfs(URI)
        ### print "importHDFSResult:", h2o.dump_json(importHDFSResult)
        s3nFullList = importHDFSResult['succeeded']
        ### print "s3nFullList:", h2o.dump_json(s3nFullList)

        self.assertGreater(len(s3nFullList),1,"Should see more than 1 files in s3n?")

        trial = 0
        for (trainCsvFilename, testCsvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()

            # PARSE test****************************************
            s3nKey = URI + testCsvFilename
            testKey2 = testCsvFilename + "_" + str(trial) + ".hex"
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            start = time.time()
            parseResult = h2o.nodes[0].parse(s3nKey, testKey2,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120)
            elapsed = time.time() - start
            print "parse end on ", s3nKey, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # PARSE train****************************************
            s3nKey = URI + trainCsvFilename
            trainKey2 = trainCsvFilename + "_" + str(trial) + ".hex"
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            start = time.time()
            parseResult = h2o.nodes[0].parse(s3nKey, trainKey2,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120)
            elapsed = time.time() - start
            print "parse end on ", s3nKey, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']


            # GLM****************************************
            y = 0 # first column is pixel value
            print "y:"
            # don't need the intermediate Dicts produced from columnInfoFromInspect
            x = h2o_glm.goodXFromColumnInfo(y, key=parseResult['destination_key'], timeoutSecs=300)
            print "x:", x

            kwargs = {
                'x': x, 
                'y': y,
                # 'case_mode': '>',
                # 'case': 0,
                'family': 'gaussian',
                'lambda': 1.0E-5,
                'alpha': 0.5,
                'max_iter': 5,
                'thresholds': 0.5,
                'n_folds': 1,
                'weight': 1,
                'beta_epsilon': 1.0E-4,
                }

            timeoutSecs = 1800
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseResult=parseResult, timeoutSecs=timeoutSecs, pollTimeoutsecs=60, **kwargs)
            elapsed = time.time() - start
            print "GLM completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            GLMModel = glm['GLMModel']
            modelKey = GLMModel['model_key']

            kwargs = {'x': x, 'y':  y, 'thresholds': 0.5}
            start = time.time()
            glmScore = h2o_cmd.runGLMScore(key=testKey2, model_key=modelKey, thresholds="0.5",
                timeoutSecs=60)
            print "GLMScore in",  (time.time() - start), "secs", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o.verboseprint(h2o.dump_json(glmScore))


if __name__ == '__main__':
    h2o.unit_main()
