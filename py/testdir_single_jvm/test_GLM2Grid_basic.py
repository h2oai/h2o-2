import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2Grid_basic_benign(self):
        csvFilename = "benign.csv"
        print "\nStarting", csvFilename 
        csvPathname = 'logreg/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        # columns start at 0
        # cols 0-13. 3 is output
        # no member id in this one
        y = "3"
        print "y:", y
        
        kwargs = {
            'ignored_cols': '0,1', 
            'response':  y, 
            'n_folds': 0, 
            'lambda': '1e-8:1e-2:100', 
            'alpha': '0,0.5,1',
            }
        # fails with n_folds
        print "Not doing n_folds with benign. Fails with 'unable to solve?'"
        # the gridded params make it grid..just call GLM2
        gg = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=120, **kwargs)
        # check the first in the models list. It should be the best
        colNames = [ 'STR','OBS','AGMT','FNDX','HIGD','DEG','CHK',
                     'AGP1','AGMN','NLV','LIV','WT','AGLP','MST' ]

        h2o_glm.simpleCheckGLMGrid(self, gg, None, **kwargs)

    def test_GLMGrid_basic_prostate(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        # columns start at 0
        csvPathname = 'logreg/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')

        y = "1"
        # 0. member ID. not used.
        # 1 is output

        print "y:", y

        # FIX! thresholds is used in GLMGrid. threshold is used in GLM
        # comma separated means use discrete values
        # colon separated is min/max/step
        # FIX! have to update other GLMGrid tests
        kwargs = {
            'ignored_cols': 0, 
            'response':  y, 
            'n_folds': 2, 
            'lambda': '1e-8:1e3:100', 
            'alpha': '0,0.5,1',
            }

        # the gridded params make it grid..just call GLM2
        gg = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=120, **kwargs)
        colNames = ['D','CAPSULE','AGE','RACE','DPROS','DCAPS','PSA','VOL','GLEASON']
        # h2o_glm.simpleCheckGLMGrid(self, gg, colNames[xList[0]], **kwargs)
        h2o_glm.simpleCheckGLMGrid(self, gg, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
