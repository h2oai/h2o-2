import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

RANDOM_UDP_DROP = False
DISABLE_ASSERTIONS = False
if RANDOM_UDP_DROP:
    print "random_udp_drop!!"

NAME_NODE = 'mr-0x6'
VERSION = 'cdh4'
# NAME_NODE = 'mr-0xd6'
# VERSION = 'hdp2.1'


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

    def test_parse_airline_multi_hdfs(self):
        csvFilename = "hex_10"
        csvFilePattern = '*' # all files in the folder

        trialMax = 2
        for tryHeap in [24]:
            print "\n", tryHeap,"GB heap, 1 jvm per host, import mr-0x6 hdfs, then parse"
            h2o.init(java_heap_GB=tryHeap, random_udp_drop=RANDOM_UDP_DROP, disable_assertions=DISABLE_ASSERTIONS,
                    use_hdfs=True, hdfs_name_node=NAME_NODE, hdfs_version=VERSION)

            timeoutSecs = 3600
            importFolderPath = "datasets/airlines_multi"

            for trial in range(trialMax):
                hex_key = csvFilename + "_" + str(trial) + ".hex"
                csvPathname = importFolderPath + "/" + csvFilePattern
                start = time.time()
                importResult = h2i.import_only(path=csvPathname, schema='hdfs', 
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
                print "importResult:", h2o.dump_json(importResult)

                parseResult = h2i.parse_only(pattern='*csv', hex_key=hex_key,
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120)
                elapsed = time.time() - start

                print "parse result:", parseResult['destination_key']
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                h2o_cmd.runStoreView()
                # we don't delete the hex key. it will start spilling? slow

            h2o.tear_down_cloud()
            # sticky ports? wait a bit.
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
