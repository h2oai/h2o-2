import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_summary_zip_s3n_fvec(self):
        csvFilelist = [
            ("test_set.zip",   300), # 110.9MB
            ("train_set.zip",  600), # 362.9MB
        ]

        (importResult, importPattern) = h2i.import_only(bucket='h2o-datasets', path="allstate", schema='s3n')

        print "\nTrying StoreView after the import hdfs"
        h2o_cmd.runStoreView(timeoutSecs=120)

        trial = 0
        for (csvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()
            csvPathname = csvFilename

            # PARSE****************************************
            csvPathname = "allstate/" + csvFilename
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket='h2o-datasets', path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120)
            elapsed = time.time() - start
            print "parse end on ", parseResult['destination_key'], 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # INSPECT******************************************
            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=360)
            print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)

            summaryResult = h2o_cmd.runSummary(key=hex_key, timeoutSecs=360)
            h2o_cmd.infoFromSummary(summaryResult)
            # STOREVIEW***************************************
            print "\nTrying StoreView after the parse"
            h2o_cmd.runStoreView(timeoutSecs=120)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
