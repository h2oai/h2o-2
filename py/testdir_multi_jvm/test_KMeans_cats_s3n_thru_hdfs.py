
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_kmeans
import h2o_browse as h2b, h2o_import as h2i
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

    def test_KMeans_allstate_s3n_thru_hdfs(self):
        # csvFilename = "covtype20x.data"
        # csvPathname = csvFilename
        csvFilename = "CAT*"
        csvPathname = "cats/" + csvFilename
        # https://s3.amazonaws.com/home-0xdiag-datasets/allstate/train_set.csv
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
            timeoutSecs = 600
            start = time.time()
            parseKey = h2o.nodes[0].parse(s3nKey, key2,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60, noise=('JStack', None))
            elapsed = time.time() - start
            print s3nKey, 'h2o reported parse time:', parseKey['response']['time']
            print "parse end on ", s3nKey, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            print "parse result:", parseKey['destination_key']

            kwargs = {
                'cols': None,
                'epsilon': 1e-6,
                'k': 12
            }

            start = time.time()
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=120, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

            ### print h2o.dump_json(kmeans)
            inspect = h2o_cmd.runInspect(None,key=kmeans['destination_key'])
            print h2o.dump_json(inspect)

            print "Deleting key in H2O so we get it from S3 (if ec2) or nfs again.", \
                  "Otherwise it would just parse the cached key."
            storeView = h2o.nodes[0].store_view()
            # pattern matching problem
            # h2o removes key afte parse now
            ### print "Removing", s3nKey
            ### removeKeyResult = h2o.nodes[0].remove_key(key=s3nKey)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds.", \


if __name__ == '__main__':
    h2o.unit_main()
