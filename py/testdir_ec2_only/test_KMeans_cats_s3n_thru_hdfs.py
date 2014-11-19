import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_kmeans, h2o_browse as h2b, h2o_import as h2i

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

    def test_KMeans_allstate_s3n_thru_hdfs(self):
        bucket = 'home-0xdiag-datasets'
        csvFilename = "CAT*"
        importFolderPath = "cats"
        csvPathname = importFolderPath + "/" + csvFilename
        timeoutSecs = 600
        trialMax = 1
        for trial in range(trialMax):
            trialStart = time.time()
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='s3n', hex_key=hex_key,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60))
            elapsed = time.time() - start
            print "parse end on ", hex_key, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            kwargs = {
                'cols': None,
                'initialization': 'Furthest',
                'k': 12
            }

            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=120, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvFilename, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

            ### print h2o.dump_json(kmeans)
            inspect = h2o_cmd.runInspect(None,key=kmeans['destination_key'])
            print h2o.dump_json(inspect)

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds.", \

if __name__ == '__main__':
    h2o.unit_main()
