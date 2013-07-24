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
        csvPathname = h2o.find_file('smalldata/winesPCA.csv')
        start = time.time()
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=10)
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

        timeoutSecs = 480

        # try the same thing 5 times
        for trial in range (10):
            start = time.time()
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "kmeans #", trial, "end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
            (centers, tupleResultList) = \
                h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseKey, 'd', **kwargs)
            # tupleResultList has tuples = center, rows_per_cluster, sqr_error_per_cluster

            # sort by centers
            from operator import itemgetter
            tupleResultList.sort(key=itemgetter(0))

            # now compare expected vs actual. By sorting on center, we should be able to compare
            # since the centers should be separated enough to have the order be consistent
            expected = [
                ([-2.2824436059344264, -0.9572469619836067], 61, 71.04484889371177),
                ([0.04072444664179102, 1.738305108029851], 67, 118.83608173427331),
                ([2.7300104405999996, -1.16148755108], 50, 68.67496427685141)

            ]
            print "\nExpected:"
            for e in expected:
                print e

            # now compare to expected, with some delta allowed
            print "\nActual:"
            for t in tupleResultList:
                print t

            for i, (expCenter, expRows, expError)  in enumerate(expected):
                (actCenter, actRows, actError) = tupleResultList[i]
                for (a,b) in zip(expCenter, actCenter):
                    self.assertAlmostEqual(a, b, delta=0.2, 
                        msg="Trial %d Center expected: %s actual: %s Too big a mismatch" % (trial, expCenter, actCenter))
                self.assertAlmostEqual(expRows, actRows, delta=7, 
                    msg="Trial %d Rows expected: %s actual: %s Too big a mismatch" % (trial, expRows, actRows))
                self.assertAlmostEqual(expError, actError, delta=2, 
                    msg="Trial %d Error expected: %s actual: %s Too big a mismatch" % (trial, expError, actError))
	    

if __name__ == '__main__':
    h2o.unit_main()
