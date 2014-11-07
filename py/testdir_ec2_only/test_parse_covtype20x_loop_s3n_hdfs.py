import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        # the node state is gone when we tear down the cloud, so pass the ignore here also.
        h2o.tear_down_cloud(sandboxIgnoreErrors=True)

    def test_parse_covtype20x_loop_s3n_hdfs(self):
        bucket = 'home-0xdiag-datasets'
        importFolderPath = "standard"
        csvFilename = "covtype20x.data"
        csvPathname = importFolderPath + "/" + csvFilename
        timeoutSecs = 500
        trialMax = 3
        for tryHeap in [4,12]:
            print "\n", tryHeap,"GB heap, 1 jvm per host, import folder,", \
                "then parse 'covtype20x.data'"
            h2o.init(java_heap_GB=tryHeap)
            # don't raise exception if we find something bad in h2o stdout/stderr?
            h2o.nodes[0].sandboxIgnoreErrors = True

            for trial in range(trialMax):
                hex_key = csvFilename + ".hex"
                start = time.time()
                parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='s3n', hex_key=hex_key,
                    timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
                elapsed = time.time() - start
                print "parse result:", parseResult['destination_key']
                print "Trial #", trial, "completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                removeKeyResult = h2o.nodes[0].remove_key(key=hex_key)

            h2o.tear_down_cloud()
            # sticky ports? wait a bit.
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
