import unittest, sys, random, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass
        print "Will build clouds with incrementing heap sizes and import folder/parse"

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_covtype20x_loop_fvec(self):
        csvFilename = "covtype20x.data"
        importFolderPath = "standard"
        csvPathname = importFolderPath + "/" + csvFilename

        trialMax = 2
        for tryJvms in [1,2,3,4]:
            for tryHeap in [3]:
                print "\n", tryHeap,"GB heap,", tryJvms, "jvm per host, import folder,", \
                    "then loop parsing 'covtype20x.data' to unique keys"
                h2o.init(node_count=tryJvms, java_heap_GB=tryHeap)
                timeoutSecs=300
                for trial in range(trialMax):
                    # since we delete the key, we have to re-import every iteration, to get it again
                    hex_key = csvFilename + "_" + str(trial) + ".hex"
                    start = time.time()
                    parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key,
                        timeoutSecs=timeoutSecs, retryDelaySecs=4, pollTimeoutSecs=60)
                    elapsed = time.time() - start
                    print "Trial #", trial, "completed in", elapsed, "seconds.", \
                        "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
                # sticky ports?
                h2o.tear_down_cloud()
                time.sleep(tryJvms * 5)

if __name__ == '__main__':
    h2o.unit_main()
