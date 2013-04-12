import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        # the node state is gone when we tear down the cloud, so pass the ignore here also.
        h2o.tear_down_cloud(sandbox_ignore_errors=True)

    def test_import_covtype20x_parse_loop(self):
        csvFilename = "covtype20x.data"
        importFolderPath = "/home/0xdiag/datasets"
        trialMax = 3
        for tryHeap in [4,12]:
            print "\n", tryHeap,"GB heap, 1 jvm per host, import folder,", \
                "then parse 'covtype20x.data'"
            h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=tryHeap,
                # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
                # this is for our amazon ec hdfs
                # see https://github.com/0xdata/h2o/wiki/H2O-and-s3n
                hdfs_name_node='10.78.14.235:9000',
                hdfs_version='0.20.2')

            # don't raise exception if we find something bad in h2o stdout/stderr?
            h2o.nodes[0].sandbox_ignore_errors = True

            timeoutSecs = 500
            URI = "s3n://home-0xdiag-datasets"
            s3nKey = URI + "/" + csvFilename
            for trial in range(trialMax):
                # since we delete the key, we have to re-import every iteration, to get it again
                # s3n URI thru HDFS is not typical.
                importHDFSResult = h2o.nodes[0].import_hdfs(URI)
                s3nFullList = importHDFSResult['succeeded']
                ### print "s3nFullList:", h2o.dump_json(s3nFullList)
                # error if none? 
                self.assertGreater(len(s3nFullList),8,"Didn't see more than 8 files in s3n?")

                key2 = csvFilename + "_" + str(trial) + ".hex"
                print "Loading s3n key: ", s3nKey, 'thru HDFS'
                start = time.time()
                parseKey = h2o.nodes[0].parse(s3nKey, key2,
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
                elapsed = time.time() - start

                print s3nKey, 'parse time:', parseKey['response']['time']
                print "parse result:", parseKey['destination_key']
                print "Trial #", trial, "completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                print "Deleting key in H2O so we get it from S3 (if ec2) or nfs again.", \
                      "Otherwise it would just parse the cached key."
                storeView = h2o.nodes[0].store_view()
                ### print "storeView:", h2o.dump_json(storeView)
                print "Removing", s3nKey
                removeKeyResult = h2o.nodes[0].remove_key(key=s3nKey)
                ### print "removeKeyResult:", h2o.dump_json(removeKeyResult)

            h2o.tear_down_cloud()
            # sticky ports? wait a bit.
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
