import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(filename='benchmark.log',
            level=logging.CRITICAL,
            format='%(asctime)s %(message)s')
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        # the node state is gone when we tear down the cloud, so pass the ignore here also.
        h2o.tear_down_cloud(sandbox_ignore_errors=True)

    def test_parse_nflx_loop_s3n_hdfs(self):
        # typical size of the michal files
        avgMichalSize = 116561140
        csvFilenameList = [
            # ("manyfiles-nflx-gz/file_1[0-9].dat.gz", "file_10.dat.gz"),
            # 100 files takes too long on two machines?
            # I use different files to avoid OS caching effects
            ("manyfiles-nflx-gz/file_1.dat.gz", "file_1.dat.gz", 1 * avgMichalSize),
            ("manyfiles-nflx-gz/file_[2][0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize),
            ("manyfiles-nflx-gz/file_[34][0-9].dat.gz", "file_20.dat.gz", 20 * avgMichalSize),
            ("manyfiles-nflx-gz/file_[5-9][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize),
            ("manyfiles-nflx-gz/file_*.dat.gz", "file_100.dat.gz", 100 * avgMichalSize),
        ]

        print "Using the -.gz files from s3"
        # want just s3n://home-0xdiag-datasets/manyfiles-nflx-gz/file_1.dat.gz
        URI = "s3n://home-0xdiag-datasets"

        # split out the pattern match and the filename used for the hex
        trialMax = 1
        for csvFilepattern, csvFilename, totalBytes in csvFilenameList:
            s3nKey = URI + "/" + csvFilepattern
            for tryHeap in [28]:
                print "\n", tryHeap,"GB heap, 1 jvm per host, import hdfs/s3n, then parse"
                h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=tryHeap,
                    # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
                    # this is for our amazon ec hdfs
                    # see https://github.com/0xdata/h2o/wiki/H2O-and-s3n
                    hdfs_name_node='10.78.14.235:9000',
                    hdfs_version='0.20.2')

                # don't raise exception if we find something bad in h2o stdout/stderr?
                h2o.nodes[0].sandbox_ignore_errors = True

                timeoutSecs = 1800
                for trial in range(trialMax):
                    # since we delete the key, we have to re-import every iteration, to get it again
                    # s3n URI thru HDFS is not typical.
                    importHDFSResult = h2o.nodes[0].import_hdfs(URI)
                    s3nFullList = importHDFSResult['succeeded']
                    for k in s3nFullList:
                        key = k['key']
                        # just print the first tile
                        if 'nflx' in key and 'file_1.dat.gz' in key: 
                            # should be s3n://home-0xdiag-datasets/manyfiles-nflx-gz/file_1.dat.gz
                            print "example file we'll use:", key

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
                    print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                        "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                    if totalBytes is not None:
                        fileMBS = (totalBytes/1e6)/elapsed
                        print "\nMB/sec (before uncompress)", "%6.2f" % fileMBS
                        l = str(len(h2o.nodes))
                        logging.critical('{:s} {:s} {:s} {:6.2f} MB/sec'.format(
                            l, csvFilepattern, csvFilename, fileMBS))


                    print "Deleting key in H2O so we get it from S3 (if ec2) or nfs again.", \
                          "Otherwise it would just parse the cached key."

                    storeView = h2o.nodes[0].store_view()
                    ### print "storeView:", h2o.dump_json(storeView)
                    # "key": "s3n://home-0xdiag-datasets/manyfiles-nflx-gz/file_84.dat.gz"
                    # have to do the pattern match ourself, to figure out what keys to delete
                    # we're deleting the keys in the initial import. We leave the keys we created
                    # by the parse. We use unique dest keys for those, so no worries.
                    # Leaving them is good because things fill up! (spill)
                    for k in s3nFullList:
                        deleteKey = k['key']
                        if csvFilename in deleteKey and not ".hex" in key:      
                            print "Removing", deleteKey
                            removeKeyResult = h2o.nodes[0].remove_key(key=deleteKey)
                            ### print "removeKeyResult:", h2o.dump_json(removeKeyResult)

                h2o.tear_down_cloud()
                # sticky ports? wait a bit.
                time.sleep(120)

if __name__ == '__main__':
    h2o.unit_main()
