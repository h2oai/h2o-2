
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b
import h2o_import as h2i
import time, random

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

    def test_parse_covtype20x_s3n_thru_hdfs(self):
        csvFilename = "covtype20x.data"
        csvPathname = csvFilename
        # csvFilename = "train_set.csv"
        # csvPathname = "allstate/ + csvFilename
        # https://s3.amazonaws.com/home-0xdiag-datasets/allstate/train_set.csv
        URI = "s3n://home-0xdiag-datasets/"
        s3nKey = URI + csvPathname

        trialMax = 3

        for trial in range(trialMax):
            trialStart = time.time()
            # since we delete the key, we have to re-import every iteration
            # s3n URI thru HDFS is not typical.
            importHDFSResult = h2o.nodes[0].import_hdfs(URI)
            s3nFullList = importHDFSResult['succeeded']
            ### print "s3nFullList:", h2o.dump_json(s3nFullList)
            self.assertGreater(len(s3nFullList),8,"Didn't see more than 8 files in s3n?")

            key2 = csvFilename + "_" + str(trial) + ".hex"
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            timeoutSecs = 500
            start = time.time()
            parseKey = h2o.nodes[0].parse(s3nKey, key2,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
            elapsed = time.time() - start
            print s3nKey, 'h2o reported parse time:', parseKey['response']['time']
            print "parse end on ", s3nKey, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            print "parse result:", parseKey['destination_key']

            kwargs = {
                'y': 54,
                'family': 'binomial',
                'link': 'logit',
                'n_folds': 2,
                'case_mode': '=',
                'case': 1,
                'max_iter': 8,
                'beta_epsilon': 1e-3}

            timeoutSecs = 720
            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, 
                initialDelaySecs=15, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time()
            print "glm (L2) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            # Elastic
            kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time()
            print "glm (Elastic) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            # L1
            kwargs.update({'alpha': 1.0, 'lambda': 1e-4})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time()
            print "glm (L1) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, 13, **kwargs)
            h2o.check_sandbox_for_errors()

            print "Deleting key in H2O so we get it from S3 (if ec2) or nfs again.", \
                  "Otherwise it would just parse the cached key."
            storeView = h2o.nodes[0].store_view()
            ### print "storeView:", h2o.dump_json(storeView)
            # h2o removes key after parse now
            ### print "Removing", s3nKey
            ### removeKeyResult = h2o.nodes[0].remove_key(key=s3nKey)
            ### print "removeKeyResult:", h2o.dump_json(removeKeyResult)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds.", \


if __name__ == '__main__':
    h2o.unit_main()
