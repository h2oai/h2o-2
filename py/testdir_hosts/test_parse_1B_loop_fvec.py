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

    def test_parse_1B_loop_fvec(self):
        csvFilename = "billion_rows.csv.gz"
        importFolderPath = "standard"
        csvPathname = importFolderPath + "/" + csvFilename
        trialMax = 3
        for tryHeap in [16]:
            print "\n", tryHeap,"GB heap, 1 jvm per host, import folder,", \
                "then loop parsing 'billion_rows.csv' to unique keys"
            h2o.init(1, java_heap_GB=tryHeap)
            timeoutSecs=800
            for trial in range(trialMax):
                hex_key = csvFilename + "_" + str(trial) + ".hex"
                start = time.time()
                parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key, 
                    timeoutSecs=timeoutSecs, retryDelaySecs=4, pollTimeoutSecs=60, doSummary=True)
                elapsed = time.time() - start
                print "Trial #", trial, "completed in", elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                print "Deleting key in H2O so we get it from S3 (if ec2) or nfs again. ", \
                      "Otherwise it would just parse the cached key."
                storeView = h2o.nodes[0].store_view()
                ### print "storeView:", h2o.dump_json(storeView)

            # sticky ports?
            h2o.tear_down_cloud()
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
