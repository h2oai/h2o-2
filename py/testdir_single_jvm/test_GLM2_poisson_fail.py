import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

print "Just do a failing case"
def define_params(): 
    paramDict = {'destination_key': None, 'standardize': None, 'family': 'poisson', 'beta_epsilon': None, 'max_iter': None, 'higher_accuracy': None, 'tweedie_variance_power': None, 'lambda_search': 1, 'ignored_cols': 0, 'source': u'covtype.20k.hex', 'n_folds': 1, 'alpha': 0.8, 'use_all_factor_levels': None, 'response': 54, 'lambda': 0}

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

    def test_GLM2_poisson_fail(self):
        csvPathname = 'covtype/covtype.20k.data'
        hex_key = 'covtype.20k.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put')
        params = define_params()
        for trial in range(3):
            kwargs = params.copy()
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=180, parseResult=parseResult, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            h2o.check_sandbox_for_errors()
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
