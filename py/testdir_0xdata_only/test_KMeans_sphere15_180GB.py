import unittest, time, sys, random, math
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_import as h2i

FROM_HDFS = False
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_sphere15_180GB(self):
        csvFilename = 'syn_sphere15_180GB.csv'
        if FROM_HDFS:
            importFolderPath = "/datasets/kmeans_big"
        else:
            importFolderPath = "/home/0xdiag/datasets/kmeans_big"
        csvPathname = importFolderPath + '/' + csvFilename

        for trial in range(2):
            # IMPORT**********************************************
            # since H2O deletes the source key, re-import every iteration.
            if FROM_HDFS:
                importFolderResult = h2i.setupImportHdfs(None, importFolderPath)
            else:
                importFolderResult = h2i.setupImportFolder(None, importFolderPath)

            # PARSE ****************************************
            print "Parse starting: " + csvFilename
            key2 = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            timeoutSecs = 300
            kwargs = {}
            if FROM_HDFS:
                parseKey = h2i.parseImportHdfsFile(None, csvFilename, importFolderPath, key2=key2,
                    timeoutSecs=timeoutSecs, pollTimeoutsecs=60, retryDelaySecs=2, **kwargs)
            else:
                parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2,
                    timeoutSecs=timeoutSecs, pollTimeoutsecs=60, retryDelaySecs=2, **kwargs)

            # KMeans ****************************************
            print "col 0 is enum in " + csvFilename + " but KMeans should skip that automatically?? or no?"
            kwargs = {
                'k': 15, 
                'initialization': 'Furthest',
                'epsilon': 1e-6, 
                'cols': None, 
                'destination_key': 'junk.hex', 
                # reuse the same seed, to get deterministic results
                'seed': 265211114317615310,
                }

            timeoutSecs = 90
            start = time.time()
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            (centers, tupleResultList)  = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseKey, 'd', **kwargs)

            # FIX! put right values in
            expected = [
                ([36.0, -17.77036285732028, 36.63308563267165, 1632.6827637598062, 13546347.0, 18548.0], 48311, 493750.51884668943) ,
                ([36.0, -16.145263418253773, 31.286000494682167, 1631.108261192184, 13546347.0, 18548.0], 40430, 408820.9849864056) ,
                ([36.0, -15.893159593312465, 36.269054378685304, 1625.9944917619082, 13546347.0, 18548.0], 41211, 415149.056368448) ,
                ([36.0, -15.047952784950203, 41.57242978342204, 1629.6467039047268, 13546347.0, 18548.0], 37954, 386293.7691152525) ,
                ([36.0, -12.502559726962458, 40.90298132905039, 1635.7641035936558, 13546347.0, 18548.0], 39848, 404349.2385063274) ,
                ([36.0, -12.42446514063435, 34.64658138839019, 1637.6381283662477, 13546347.0, 18548.0], 53472, 596025.7936677273) ,
                ([36.0, -11.35396425572748, 31.056759026028548, 1626.0950221902363, 13546347.0, 18548.0], 41685, 419146.5826556266) ,
                ([36.0, -10.874608577263444, 36.06194690265487, 1631.0853058445978, 13546347.0, 18548.0], 51415, 393534.14917824126) ,
                ([36.0, -10.714559229217278, 29.176822991723967, 1632.7038127393255, 13546347.0, 18548.0], 48574, 498571.3849178554) ,
                ([36.0, -9.448876334185544, 37.07609103749128, 1624.343446623639, 13546347.0, 18548.0], 55933, 643531.6087819397) ,
                ([36.0, -9.355590599638447, 42.49388438016847, 1629.4287087964922, 13546347.0, 18548.0], 51998, 570181.9085541677) ,
                ([36.0, -6.866317483139913, 40.20701105747835, 1635.1972202252575, 13546347.0, 18548.0], 29211, 282336.9881893812) ,
                ([36.0, -6.246933497221582, 34.357634887710944, 1635.909477350132, 13546347.0, 18548.0], 39051, 392172.09387725615) ,
                ([36.0, -5.714896796969619, 31.570051794779292, 1629.5981910480068, 13546347.0, 18548.0], 38807, 398328.5253691258) ,
                ([36.0, -4.173732335827099, 37.271374409504695, 1629.8530097520427, 13546347.0, 18548.0], 49323, 510001.2618453919) ,
            ]
            # all are multipliers of expected tuple value
            allowedDelta = (0.01, 0.01, 0.01) 
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)

if __name__ == '__main__':
    h2o.unit_main()
