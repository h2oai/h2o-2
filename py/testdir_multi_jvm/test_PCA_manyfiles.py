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

    def test_PCA_manyfiles(self):
        bucket = 'home-0xdiag-datasets'
        modelKey = 'GBMModelKey'
        files = [
                # None forces num_cols to be used. assumes you set it from Inspect
                ('manyfiles-nflx-gz', 'file_1.dat.gz', 'file_1.hex', 1800)
                ]

        # if I got to hdfs, it's here
        # hdfs://192.168.1.176/datasets/manyfiles-nflx-gz/file_99.dat.gz

        # h2b.browseTheCloud()
        for (importFolderPath, csvFilename, hexKey, timeoutSecs) in files:
            h2o.beta_features = False #turn off beta_features
            # PARSE train****************************************
            start = time.time()
            xList = []
            eList = []
            fList = []

            # Parse (train)****************************************
            if h2o.beta_features:
                print "Parsing to fvec directly! Have to noPoll=true!, and doSummary=False!"
            csvPathname = importFolderPath + "/" + csvFilename
            parseResult = h2i.import_parse(bucket=bucket, path=csvPathname, schema='local',
                hex_key=hexKey, timeoutSecs=timeoutSecs, noPoll=h2o.beta_features, doSummary=False)

            # hack
            if h2o.beta_features:
                h2j.pollWaitJobs(timeoutSecs=timeoutSecs, pollTimeoutSecs=timeoutSecs)
                print "Filling in the parseResult['destination_key'] for h2o"
                parseResult['destination_key'] = hexKey

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

            # if you set beta_features here, the fvec translate will happen with the Inspect not the PCA
            # h2o.beta_features = True
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])
            num_rows = inspect['num_rows']
            num_cols = inspect['num_cols']

            # PCA(tolerance iterate)****************************************
            #h2o.beta_features = True
            for tolerance in [i/10.0 for i in range(11)]:
                params = {
                    'destination_key': modelKey,
                    'ignore': 0,
                    'tolerance': tolerance,
                    'standardize': 1,
                }
                print "Using these parameters for PCA: ", params
                kwargs = params.copy()
                #h2o.beta_features = True

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
                #h2o.beta_features = True
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
                #h2o.beta_features=False

if __name__ == '__main__':
    h2o.unit_main()
