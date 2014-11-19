import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_allstate_s3n_thru_hdfs(self):
        # csvFilename = "covtype20x.data"
        # csvPathname = csvFilename
        bucket = 'home-0xdiag-datasets'
        csvFilename = "train_set.csv"
        hex_key = "train_set.hex"
        csvPathname = "allstate/" + csvFilename
        timeoutSecs = 500
        trialMax = 3
        for trial in range(trialMax):
            trialStart = time.time()
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
            elapsed = time.time() - start
            print "parse end on ", hex_key, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

if __name__ == '__main__':
    h2o.unit_main()
