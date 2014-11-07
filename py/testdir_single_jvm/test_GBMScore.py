import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_gbm, h2o_jobs as h2j, h2o_import

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        h2o.init(java_heap_GB=10)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBMScore(self):
        importFolderPath = 'standard'
        csvTrainPath = importFolderPath + '/allyears2k.csv'
        csvTestPath = csvTrainPath
#        importFolderPath = 'newairlines'
#        csvTrainPath = importFolderPath + '/train/*train*'
#        csvTestPath  = importFolderPath + '/train/*test*'
        trainhex = 'train.hex'
        testhex = 'test.hex'
        parseTrainResult = h2i.import_parse(bucket='home-0xdiag-datasets', path = csvTrainPath, schema = 'local', hex_key = trainhex, timeoutSecs = 2400, doSummary = False)
        parseTestResult = h2i.import_parse(bucket='home-0xdiag-datasets', path = csvTestPath, schema = 'local', hex_key = testhex, timeoutSecs = 2400, doSummary = False)
        inspect_test   = h2o.nodes[0].inspect(testhex, timeoutSecs=8000)
        response = 'IsDepDelayed'
        ignored_cols = 'DepTime,ArrTime,FlightNum,TailNum,ActualElapsedTime,AirTime,ArrDelay,DepDelay,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed'


        params   =  {'destination_key'      : 'GBMScore',
                     'response'             : response,
                     'ignored_cols_by_name' : ignored_cols,
                     'classification'       : 1,
                     'validation'           : None,
                     'ntrees'               : 100,
                     'max_depth'            : 10,
                     'learn_rate'           : 0.00005,
                    }

        parseResult = {'destination_key' : trainhex}
        kwargs    = params.copy()
        gbm = h2o_cmd.runGBM(parseResult = parseResult, timeoutSecs=4800, **kwargs)

        scoreStart = time.time()
        h2o.nodes[0].generate_predictions(model_key = 'GBMScore', data_key = trainhex)
        scoreElapsed = time.time() - scoreStart

        print "It took ", scoreElapsed, " seconds to score ", inspect_test['numRows'], " rows. Using a GBM with 100 10-deep trees."
        print "That's ", 1.0*scoreElapsed / 100.0  ," seconds per 10-deep tree."


if __name__ == '__main__':
    h2o.unit_main()
