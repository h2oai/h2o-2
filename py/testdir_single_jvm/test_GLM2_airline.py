import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_glm
from pprint import pprint 

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    #test_players_NA
    def test_GLM2_airline(self):
        #############Train###############################
        csvFilename = 'AirlinesTrain.csv.zip'
        csvPathname = 'airlines'+'/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)
        params = {'response': 'IsDepDelayed', 'ignored_cols': 'IsDepDelayed_REC', 'family': 'binomial'}
        kwargs = params.copy()
        starttime = time.time()
        glmtest = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
        elapsedtime = time.time() - starttime 
        print("ELAPSED TIME TRAIN DATA ",elapsedtime)
        h2o_glm.simpleCheckGLM(self, glmtest, None, **kwargs)

      

        ######### Test ######################################
        csvFilename = 'AirlinesTest.csv.zip'
        csvPathname = 'airlines'+'/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)
        params = {'response': 'IsDepDelayed', 'ignored_cols': 'IsDepDelayed_REC', 'family': 'binomial'}
        kwargs = params.copy()
        starttime = time.time()
        glmtrain = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
        elapsedtime = time.time() - starttime
        print("ELAPSED TIME TEST DATA ",elapsedtime)
        h2o_glm.simpleCheckGLM(self, glmtrain, None, **kwargs)

#########End of Test####################################

if __name__ == '__main__':
    h2o.unit_main()
