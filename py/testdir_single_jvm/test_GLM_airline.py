import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o_hosts,h2o_glm
import h2o, h2o_cmd, h2o_import as h2i
from pprint import pprint 

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud()
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

#test_players_NA
    def test_GLM_airline(self):
#############Train###############################
        csvFilename = 'AirlinesTrain.csv.zip'
        csvPathname = 'airlines'+'/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)
        params = {'y': 'IsDepDelayed', 'x':'fYear,fMonth,fDayofMonth,fDayOfWeek,DepTime,ArrTime,UniqueCarrier,Origin,Dest,Distance', 'n_folds': 3, 'family': "gaussian", 'alpha': 0.5, 'lambda': 1e-4, 'max_iter': 30}
        kwargs = params.copy()
        starttime = time.time()
        glmtest = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
        elapsedtime = time.time() - starttime 
## Printing out a coefficients only.

        pprint(glmtest['GLMModel']['coefficients'])
        pprint(glmtest['GLMModel']['normalized_coefficients'])
        pprint(glmtest['GLMModel']['nCols'])
      

        print("ELAPSED TIME TRAIN DATA ",elapsedtime)
        sys.stdout.write('.')
        sys.stdout.flush()

######### Test ######################################
        csvFilename = 'AirlinesTest.csv.zip'
        csvPathname = 'airlines'+'/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)
        params = {'y': 'IsDepDelayed', 'x':'fYear,fMonth,fDayofMonth,fDayOfWeek,DepTime,ArrTime,UniqueCarrier,Origin,Dest,Distance', 'n_folds': 3, 'family': "gaussian", 'alpha': 0.5, 'lambda': 1e-4, 'max_iter': 30}
        kwargs = params.copy()
        starttime = time.time()
        glmtrain = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)
        elapsedtime = time.time() - starttime
           
        pprint(glmtrain['GLMModel']['coefficients'])
        pprint(glmtrain['GLMModel']['normalized_coefficients'])
        print("ELAPSED TIME TEST DATA ",elapsedtime)
   
        sys.stdout.write('.')
        sys.stdout.flush()

#########End of Test####################################

if __name__ == '__main__':
    h2o.unit_main()
