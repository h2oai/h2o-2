import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_exec as h2e

def define_params():

    
    paramDict = {
        'standardize': [None, 0,1],
        'beta_epsilon': [None, 0.0001],
        'family': [None, 'gaussian', 'binomial', 'poisson'],
        'lambda': [0,1e-8,1e-4,1e-3],
        'alpha': [0,0.8,0.75],
        'ignored_cols': [1,'C1','1,2','C1,C2'],
        'max_iter': [None, 10],
        'higher_accuracy': [None, 0, 1],
        'use_all_factor_levels': [None, 0, 1],
        'lambda_search': [None, 0], # FIX! what if lambda is set when lambda_search=1
        'tweedie_variance_power': [None, 0, 1],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_lambda_search(self):
        csvPathname = 'covtype/covtype.20k.data'

        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', hex_key="covtype.20k")

        CLASS = 1
        # make a binomial version 
        execExpr="B.hex=%s; B.hex[,%s]=(B.hex[,%s]==%s)" % ('covtype.20k', 54+1, 54+1, CLASS)
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        paramDict = define_params()
        for trial in range(8):
            params = {}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            # override choices with these
            params = {
                'response': 54, 
                'alpha': 0.1, 
                'max_iter': 8,
                # 'lambda': 1e-4, 
                # 'lambda': 0,
                'lambda': None,
                'lambda_search': 1, 
                'n_folds': 1,
            }
            kwargs = params.copy()

            if 'family' not in kwargs or kwargs['family']=='binomial':
                bHack = {'destination_key': 'B.hex'}
            else:
                bHack = parseResult
            
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=300, parseResult=bHack, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            h2o.check_sandbox_for_errors()
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
