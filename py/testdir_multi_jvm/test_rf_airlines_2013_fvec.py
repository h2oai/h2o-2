import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_rf, h2o_gbm, h2o_import as h2i, h2o_browse as h2b

PARSE_TIMEOUT=14800

class Basic(unittest.TestCase):

    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1, java_heap_GB=14, enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def test_rf_airlines_2013_fvec(self):
        h2b.browseTheCloud()


        csvFilename = 'year2013.csv'
        hex_key = 'year2013.hex'
        importFolderPath = 'airlines'
        csvPathname = importFolderPath + "/" + csvFilename
        start      = time.time()
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', 
            path=csvPathname, schema='local', hex_key=hex_key, timeoutSecs=900, doSummary=False)
        parse_time = time.time() - start 
        print "parse took {0} sec".format(parse_time)
        start      = time.time()
        
        start = time.time()
        # noise=['JStack','cpu','disk'])
        h2o_cmd.runSummary(key=hex_key, timeoutSecs=200)
        elapsed = time.time() - start 
        print "summary took {0} sec".format(elapsed)

        trees = 10
        paramsTrainRF = { 
            'ntrees': trees, 
            'max_depth': 20,
            'nbins': 200,
            'ignored_cols_by_name': 'CRSDepTime,CRSArrTime,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed',
            'timeoutSecs': 14800,
            }
        kwargs   = paramsTrainRF.copy()
        start      = time.time()
        rfView = h2o_cmd.runRF(parseResult=parseResult, **kwargs)
        elapsed = time.time() - start
        (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)

        l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:.2f} secs. \
            trees: {:} classification_error: {:} classErrorPct: {:} totalScores: {:}' .format(
            len(h2o.nodes), h2o.nodes[0].java_heap_GB, 'DRF2', csvFilename, elapsed,
                trees, classification_error, classErrorPctList, totalScores)
        print "\n"+l
        h2o.cloudPerfH2O.message(l)

        # just to make sure we test this
        h2i.delete_keys_at_all_nodes(pattern=hex_key)

if __name__ == '__main__':
    h2o.unit_main()
