import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

def define_params():
    paramDict = {
        'standardize': [None, 0, 1],
        'beta_epsilon': [None, 0.0001],
        'family': ['poisson'],
        'n_folds': [2, 3, 4],
        'lambda': [1e-8, 1e-4],
        'alpha': [0, 0.5],
        'max_iter': [5, 10, 19],
        # only log and identify are legal
        # 'link': ["identify", "logit", "log", "inverse", "tweedie"]
        'link': ["identity", "log"]
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

    def test_GLM2_poisson_rand2(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        paramDict = define_params()
        for trial in range(20):
            params = {
                'response': 54, 
                'n_folds': 3, 
                'family': "poisson", 
                'alpha': 0.5, 
                'lambda': 1e-4, 
                'beta_epsilon': 0.001, 
                'max_iter': 15,
                }

            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            # make timeout bigger with xvals
            timeoutSecs = 60 + (kwargs['n_folds']*40)
            # or double the 4 seconds per iteration (max_iter+1 worst case?)
            timeoutSecs = max(timeoutSecs, (8 * (kwargs['max_iter']+1)))

            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=timeoutSecs, parseResult=parseResult, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
