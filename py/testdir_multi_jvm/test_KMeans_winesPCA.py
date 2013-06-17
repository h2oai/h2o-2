import os, json, unittest, time, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_kmeans
import h2o_browse as h2b, h2o_import as h2i

#uses the wines data from http://archive.ics.uci.edu/ml/datasets/Wine
#PCA performed to collect data into 2 rows.
#3 groups, small & easy


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_winesPCA(self):
        if localhost:
            csvFilenameList = [
                #with winesPCA2.csv speciy cols = "1,2"
                ('winesPCA.csv', 480, 'cA'),
                ]
        else:
            # None is okay for key2
            csvFilenameList = [
                ('winesPCA.csv', 480,'cA'),
                # ('covtype200x.data', 1000,'cE'),
                ]

        importFolderPath = os.path.abspath(h2o.find_file('smalldata'))
        h2i.setupImportFolder(None, importFolderPath)
        for csvFilename, timeoutSecs, key2 in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            # creates csvFilename.hex from file in importFolder dir 
            start = time.time()
            parseKey = h2i.parseImportFolderFile(None, 'winesPCA.csv', importFolderPath, 
                timeoutSecs=2000, key2=key2, noise=('JStack', None))
            print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            kwargs = {
		#appears not to take 'cols'?
                'cols': None,
                'epsilon': 1e-6,
                'k': 3
            }

            start = time.time()
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
            centers = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseKey, 'd', **kwargs)
	    print "Expected centers: [-2.276318, -0.965151], with 59 rows."
	    print "                  [0.0388763, 1.63886039], with 71 rows."
	    print "		     [2.740469, -1.237816], with 48 rows."
	    model_key = kmeans['destination_key']
	    kmeansScoreResult = h2o.nodes[0].kmeans_score(
	    	key = parseKey['destination_key'], model_key = model_key)
	    score  = kmeansScoreResult['score']
	    

if __name__ == '__main__':
    h2o.unit_main()
