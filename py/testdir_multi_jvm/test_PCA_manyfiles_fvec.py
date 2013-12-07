import unittest
import random, sys, time, re
sys.path.extend(['.','..','py'])
import h2o_browse as h2b, h2o_gbm, h2o_pca
import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util, h2o_rf, h2o_jobs as h2j
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3, java_heap_GB=4, enable_benchmark_log=True)
        else:
            h2o_hosts.build_cloud_with_hosts(enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_PCA_manyfiles_fvec(self):
        h2o.beta_features = True
        bucket = 'home-0xdiag-datasets'
        modelKey = 'PCAModelKey'
        files = [
                # None forces numCols to be used. assumes you set it from Inspect
                ('manyfiles-nflx-gz', 'file_1.dat.gz', 'file_1.hex', 1800)
                ]

        # if I got to hdfs, it's here
        # hdfs://192.168.1.176/datasets/manyfiles-nflx-gz/file_99.dat.gz

        for (importFolderPath, csvFilename, hexKey, timeoutSecs) in files:
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            # Parse (train)****************************************
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                hex_key=hexKey, timeoutSecs=timeoutSecs, doSummary=False)

            elapsed = time.time() - start
            print "parse end on ", csvPathname, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseResult['destination_key']

            # Logging to a benchmark file
            algo = "Parse"
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, elapsed)
            print l
            h2o.cloudPerfH2O.message(l)

            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numRows = inspect['numRows']
            numCols = inspect['numCols']

            ignore_x = [3,4,5,6,7,8,9,10,11,14,16,17,18,19,20,424,425,426,540,541,378]
            print ignore_x
            ignored_cols = ",".join(map(lambda x: "C" + str(x), ignore_x))
            
            # for comparison
            ignore_x = h2o_glm.goodXFromColumnInfo(378, key=parseResult['destination_key'], timeoutSecs=300, forRF=True)
            print ignore_x


            # PCA(tolerance iterate)****************************************
            for tolerance in [i/10.0 for i in range(11)]:
                params = {
                    'destination_key': modelKey,
                    'ignored_cols': ignored_cols,
                    'tolerance': tolerance,
                    'standardize': 1,
                    'max_pc': None,
                }

                print "Using these parameters for PCA: ", params
                kwargs = params.copy()
                pcaResult = h2o_cmd.runPCA(parseResult=parseResult,
                     timeoutSecs=timeoutSecs, **kwargs)
                print "PCA completed in", pcaResult['python_elapsed'], "seconds. On dataset: ", csvPathname
                print "Elapsed time was ", pcaResult['python_%timeout'], "% of the timeout"
                print "Checking PCA results: "
        
                h2o_pca.simpleCheckPCA(self,pcaResult)
                h2o_pca.resultsCheckPCA(self,pcaResult)

                # Logging to a benchmark file
                algo = "PCA " + " tolerance=" + str(tolerance)
                l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs'.format(
                    len(h2o.nodes), h2o.nodes[0].java_heap_GB, algo, csvFilename, pcaResult['python_elapsed'])
                print l
                h2o.cloudPerfH2O.message(l)
                pcaInspect = h2o_cmd.runInspect(key=modelKey)
                # errrs from end of list? is that the last tree?
                sdevs = pcaInspect["PCAModel"]["stdDev"] 
                print "PCA: standard deviations are :", sdevs
                print
                print
                propVars = pcaInspect["PCAModel"]["propVar"]
                print "PCA: Proportions of variance by eigenvector are :", propVars
                print
                print

if __name__ == '__main__':
    h2o.unit_main()
