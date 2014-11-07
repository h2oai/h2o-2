import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_jobs, h2o_gbm

DO_CLASSIFICATION = True

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GBMGrid_basic_benign(self):
        csvFilename = "benign.csv"
        print "\nStarting", csvFilename 
        csvPathname = 'logreg/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        # columns start at 0
        # cols 0-13. 3 is output
        # no member id in this one
        
        # check the first in the models list. It should be the best
        colNames = [ 'STR','OBS','AGMT','FNDX','HIGD','DEG','CHK', 'AGP1','AGMN','NLV','LIV','WT','AGLP','MST' ]
        modelKey = 'GBMGrid_benign'

        # 'cols', 'ignored_cols_by_name', and 'ignored_cols' have to be exclusive
        params = {
            'destination_key': modelKey,
            'ignored_cols_by_name': 'STR',
            'learn_rate': '.1,.2,.25',
            'ntrees': '3:5:1',
            'max_depth': '5,7',
            'min_rows': '1,2',
            'response': 'FNDX',
            'classification': 1 if DO_CLASSIFICATION else 0,
            }

        kwargs = params.copy()
        timeoutSecs = 1800
        start = time.time()
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, **kwargs)
        elapsed = time.time() - start
        print "GBM training completed in", elapsed, "seconds."
        h2o_gbm.showGBMGridResults(GBMResult, 0)

    def test_GBMGrid_basic_prostate(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        # columns start at 0
        csvPathname = 'logreg/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        colNames = ['ID','CAPSULE','AGE','RACE','DPROS','DCAPS','PSA','VOL','GLEASON']

        modelKey = 'GBMGrid_prostate'
        # 'cols', 'ignored_cols_by_name', and 'ignored_cols' have to be exclusive
        params = {
            'destination_key': modelKey,
            'ignored_cols_by_name': 'ID',
            'learn_rate': '.1,.2',
            'ntrees': '1:3:1',
            'max_depth': '8,9',
            'min_rows': '1:5:2',
            'response': 'CAPSULE',
            'classification': 1 if DO_CLASSIFICATION else 0,
            }

        kwargs = params.copy()
        timeoutSecs = 1800
        start = time.time()
        GBMResult = h2o_cmd.runGBM(parseResult=parseResult, **kwargs)
        elapsed = time.time() - start
        print "GBM training completed in", elapsed, "seconds."
        h2o_gbm.showGBMGridResults(GBMResult, 15)


if __name__ == '__main__':
    h2o.unit_main()
