import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_util

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_tweedie(self):
        csvFilename = "AutoClaim.csv"
        csvPathname = 'standard/' + csvFilename
        print "\nStarting", csvPathname
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        # columns start at 0
        # regress: glm(CLM_AMT ~ CAR_USE + REVOLKED + GENDER + AREA + MARRIED + CAR_TYPE, data=AutoClaim, family=tweedie(1.34))
        
        coefs = [7, 13, 20, 27, 21, 11]
        y = 4
        ignored_cols = h2o_cmd.createIgnoredCols(key=parseResult['destination_key'], cols=coefs, response=y)

        # sapply(c('CLM_AMT', 'CAR_USE', 'REVOLKED', 'GENDER', 'AREA', 'MARRIED', 'CAR_TYPE'), function(x) which(x==colnames(AutoClaim)) - 1)
        kwargs = {
                'family': 'tweedie',
                'tweedie_variance_power': 1.36,
                'response': y, 
                'ignored_cols' : ignored_cols,
                'max_iter': 10, 
                'lambda': 0,
                'alpha': 0,
                'n_folds': 0,
                'beta_epsilon': 1e-4,
        }

        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=15, **kwargs)

        coefficientsExpected = {'Intercept': 0, 'GENDER.M': 0.0014842488782470984, 'CAR_TYPE.Sports Car': 0.07786742314454961, 'MARRIED.Yes': 0.0007748552195851079, 'CAR_TYPE.SUV': 0.07267702940249621, 'CAR_TYPE.Pickup': 0.04952083408742968, 'CAR_TYPE.Van': 0.026422137690691405, 'CAR_TYPE.Sedan': 0.05128350794060489, 'CAR_USE.Private': -0.03050194832853935, 'REVOLKED.Yes': -0.05095942737408699}

        deltaExpected = 0.05
        (warnings, coefficients, intercept) = h2o_glm.simpleCheckGLM(self, glm, None,   
            coefficientsExpected=coefficientsExpected, deltaExpected=deltaExpected, **kwargs)
        print 'coefficients: %s' % (str(coefficients))
 
if __name__ == '__main__':
    h2o.unit_main()
