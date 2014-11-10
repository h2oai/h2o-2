import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i


DO_WEIGHT_FAIL = False
def define_params(): 
    paramDict = {
        'standardize': [None, 0,1],
        'beta_epsilon': [None, 0.0001],
        'family': ['tweedie'],
        'ignored_cols': [0,1,15,33],
        'lambda': [0, 1e-8, 1e-4],
        'alpha': [0,0.2,0.8],
        'tweedie_variance_power': [1.00001, 1.9999999],
        'max_iter': [None, 10],
        'n_folds': [None, 0, 1, 2],
        'standardize': [None, 0, 1],
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

    def test_NOPASS_GLM2_tweedie_rand2(self):
        if 1==1:
            csvPathname = 'standard/covtype.data'
            hex_key = 'covtype.hex'
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, schema='put')
        else:
            csvPathname = 'covtype/covtype.20k.data'
            hex_key = 'covtype.20k.hex'
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put')


        paramDict = define_params()

        for trial in range(10):
            # params is mutable. This is default.
            params = {
                'response': 54, 
                'lambda': 0, 
                'alpha': 0, 
                'n_folds': 1, 
                'family': 'tweedie'
            }
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=180, parseResult=parseResult, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            h2o.check_sandbox_for_errors()
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
