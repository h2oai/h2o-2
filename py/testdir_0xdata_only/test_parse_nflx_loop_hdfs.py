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

    def test_import_nflx_parse_loop(self):
        print "Using the -.gz files from hdfs"
        # hdfs://<name node>/datasets/manyfiles-nflx-gz/file_1.dat.gz
        csvFilename = "file_10.dat.gz"
        csvFilepattern = "file_1[0-9].dat.gz"

        trialMax = 2
        for tryHeap in [24]:
            print "\n", tryHeap,"GB heap, 1 jvm per host, import 192.168.1.176 hdfs, then parse"
            localhost = h2o.decide_if_localhost()
            if (localhost):
                h2o.build_cloud(node_count=1, java_heap_GB=tryHeap,
                    use_hdfs=True, hdfs_name_node='192.168.1.176', hdfs_version='cdh3')
            else:
                h2o_hosts.build_cloud_with_hosts(node_count=1, java_heap_GB=tryHeap,
                    use_hdfs=True, hdfs_name_node='192.168.1.176', hdfs_version='cdh3')

            # don't raise exception if we find something bad in h2o stdout/stderr?
            # h2o.nodes[0].sandbox_ignore_errors = True

            timeoutSecs = 500
            importFolderPath = "/datasets/manyfiles-nflx-gz"
            for trial in range(trialMax):
                # since we delete the key, we have to re-import every iteration, to get it again
                importHdfsResult = h2i.setupImportHdfs(path=importFolderPath)
                hdfsFullList = importHdfsResult['succeeded']
                for k in hdfsFullList:
                    key = k['key']
                    # just print the first tile
                    if 'nflx' in key and 'file_1.dat.gz' in key: 
                        # should be hdfs://home-0xdiag-datasets/manyfiles-nflx-gz/file_1.dat.gz
                        print "example file we'll use:", key

                ### print "hdfsFullList:", h2o.dump_json(hdfsFullList)
                # error if none? 
                self.assertGreater(len(hdfsFullList),8,"Didn't see more than 8 files in hdfs?")

                key2 = csvFilename + "_" + str(trial) + ".hex"
                csvFilePattern = 'file_1.dat.gz'
                # "key": "hdfs://192.168.1.176/datasets/manyfiles-nflx-gz/file_99.dat.gz", 

                time.sleep(5)
                print "Loading from hdfs:", importFolderPath + "/" + csvFilePattern
                start = time.time()
                parseKey = h2i.parseImportHdfsFile(csvFilename=csvFilePattern, path=importFolderPath,
                    key2=key2, timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
                elapsed = time.time() - start

                print hdfsKey, 'parse time:', parseKey['response']['time']
                print "parse result:", parseKey['destination_key']
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_cmd.runStoreView()

            h2o.tear_down_cloud()
            # sticky ports? wait a bit.
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
