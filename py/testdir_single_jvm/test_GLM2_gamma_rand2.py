import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

def define_params():
    paramDict = {
        'standardize': [None, 0,1],
        'beta_epsilon': [None, 0.0001],
        'ignored_cols': [0,1,15,33,34],
        'family': ['gamma'],
        'n_folds': [2,3,4],
        'lambda': [1e-8, 1e-4],
        'alpha': [0,0.5,0.75],
        'max_iter': [24],
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

    def test_GLM2_gamma_rand2(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        paramDict = define_params()
        for trial in range(10):
            # params is mutable. This is default.
            params = {'response': 54, 'n_folds': 3, 'family': "gamma", 'alpha': 0.5, 'lambda': 1e-4, 'max_iter': 24}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=300, parseResult=parseResult, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
