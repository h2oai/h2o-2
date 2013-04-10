
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

    def test_GLM_hhp_gz_s3n_thru_hdfs(self):
        print "Trying to load the 100 gz files in the hhp_107_01 dir"
        print "Should look like hhp_107_01/hhp_107_01.data.gz_00030 in bucket: home-0xdiag-datasets"

        csvFilename = "*gz"
        csvPathname = "hhp_107_01/" + csvFilename
        URI = "s3n://home-0xdiag-datasets/"
        s3nKey = URI + csvPathname

        trialMax = 1
        for trial in range(trialMax):
            trialStart = time.time()
            # since we delete the key, we have to re-import every iteration
            # s3n URI thru HDFS is not typical.
            importHDFSResult = h2o.nodes[0].import_hdfs(URI)
            s3nFullList = importHDFSResult['succeeded']
            ### print "s3nFullList:", h2o.dump_json(s3nFullList)
            self.assertGreater(len(s3nFullList),8,"Didn't see more than 8 files in s3n?")
            storeView = h2o.nodes[0].store_view()
            ### print "storeView:", h2o.dump_json(storeView)
            for s in storeView['keys']:
                print "\nkey:", s['key']
                if 'rows' in s:
                    print "rows:", s['rows'], "value_size_bytes:", s['value_size_bytes']

            key2 = csvFilename + "_" + str(trial) + ".hex"
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            # ec2 is about 400 secs on four m2.4xlarge nodes
            # should be less on more nodes?
            timeoutSecs = 500
            start = time.time()
            parseKey = h2o.nodes[0].parse(s3nKey, key2,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60, noise=('JStack', None))
            elapsed = time.time() - start
            print s3nKey, 'h2o reported parse time:', parseKey['response']['time']
            print "parse end on ", s3nKey, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            print "parse result:", parseKey['destination_key']

            kwargs = {
                'x': '0,1,2,3,4,5,6,7,8,10,11',
                'y': 106,
                'family': 'gaussian',
                'lambda': 1.0E-5,
                'max_iter': 50,
                'weight': 1.0,
                'thresholds': 0.5,
                'link': 'familyDefault',
                'n_folds': 0,
                'alpha': 1,
                'beta_epsilon': 1.0E-4,
                }

            timeoutSecs = 500
            # L2 
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, 
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, noise=('JStack', None), **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            h2o_glm.simpleCheckGLM(self, glm, None, noPrint=True, **kwargs)
            h2o.check_sandbox_for_errors()

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds.", \

if __name__ == '__main__':
    h2o.unit_main()
