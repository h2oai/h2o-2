import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

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

    def test_parse_syn_s3n_thru_hdfs(self):
        # I put these file copies on s3 with unique suffixes
        # under this s3n "path"
        bucket = 'home-0xdiag-datasets'
        importFolderPath = 'syn_datasets'
        csvFilename = "*_10000x200*"
        csvPathname = importFolderPath + "/" + csvFilename
        trialMax = 1
        timeoutSecs = 500
        for trial in range(trialMax):
            hex_key = "syn_datasets_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)
            elapsed = time.time() - start
            print "parse result:", parseResult['destination_key']
            print "Trial #", trial, "completed in", elapsed, "seconds.", \
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
            print "\n" + hex_key + \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])

            print "Removing", hex_key
            removeKeyResult = h2o.nodes[0].remove_key(key=hex_key)


if __name__ == '__main__':
    h2o.unit_main()
