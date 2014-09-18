import unittest, sys, random, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_hosts, h2o_jobs as h2j

print "random_udp_drop!!"
RANDOM_UDP_DROP = True
# NAME_NODE = 'mr-0x6'
# VERSION = 'cdh4'
NAME_NODE = 'mr-0xd6'
VERSION = 'hdp2.1'
TRIAL_MAX = 10

print "Using", VERSION, "on", NAME_NODE

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
        h2o.tear_down_cloud(sandboxIgnoreErrors=True)

    def test_parse_nflx_loop_hdfs_fvec(self):
        h2o.beta_features = True
        print "Using the -.gz files from hdfs"
        # hdfs://<name node>/datasets/manyfiles-nflx-gz/file_1.dat.gz

        # default
        csvFilename = "hex_10"
        csvFilePattern = '*' # all files in the folder

        for tryHeap in [24]:
            print "\n", tryHeap,"GB heap, 1 jvm per host, import mr-0x6 hdfs, then parse"
            localhost = h2o.decide_if_localhost()
            if (localhost):
                h2o.build_cloud(java_heap_GB=tryHeap, random_udp_drop=RANDOM_UDP_DROP, base_port=55930,
                    use_hdfs=True, hdfs_name_node=NAME_NODE, hdfs_version=VERSION)
            else:
                h2o_hosts.build_cloud_with_hosts(java_heap_GB=tryHeap, random_udp_drop=RANDOM_UDP_DROP, base_port=55600,
                    use_hdfs=True, hdfs_name_node=NAME_NODE, hdfs_version=VERSION)

            # don't raise exception if we find something bad in h2o stdout/stderr?
            # h2o.nodes[0].sandboxIgnoreErrors = True

            timeoutSecs = 500
            importFolderPath = "datasets/airlines_multi"
            csvPathname = importFolderPath + "/" + csvFilePattern
            parseResult = h2i.import_only(path=csvPathname, schema='hdfs',
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)

            for trial in range(TRIAL_MAX):
                # each parse now just does one
                csvFilePattern = "*%s.csv" % trial
                # if we want multifile
                # csvFilePattern = "*"

                hex_key = csvFilename + "_" + str(trial) + ".hex"
                csvPathname = importFolderPath + "/" + csvFilePattern
                start = time.time()
                # print "Don't wait for completion. Just load things up!"
    
                print "Drat. the source file is locked if we noPoll. Would have to increment across the individual files?"
                
                print "Drat. We can't re-import the folder, if there's a parse using one of the source files?"
                parseResult = h2i.parse_only(pattern=csvFilePattern, hex_key=hex_key, noPoll=True, delete_on_done=0,
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
                elapsed = time.time() - start

                print "parse result:", parseResult['destination_key']
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_cmd.runStoreView()
                # we don't delete the hex key. it will start spilling? slow

            h2j.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=30)
            h2o.tear_down_cloud()
            # sticky ports? wait a bit.
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
