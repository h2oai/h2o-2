import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_import as h2i

def define_params(): 
    paramDict = { # FIX! no ranges on X 0:3
        'standardize': [None, 0,1],
        'ignored_cols': [0,1,15,33],
        # dn't have 0/1 output
        # 'family': ['gaussian', 'binomial'],
        'family': ['gaussian', 'poisson'],
        'lambda': [0, 1e-8, 1e-4],
        'alpha': [0,0.2,0.8],
        'max_iter': [None, 10],
        'standardize': [None, 0, 1],
        'tweedie_variance_power': [None, 0, 1],
        'n_folds': [None, 0, 1, 2],
        'beta_epsilon': [None, 1e-4, 1e-8],
        'higher_accuracy': [None, 0, 1],
        'use_all_factor_levels': [None, 0, 1],
        'lambda_search': [None, 0, 1],
        }
    return paramDict
    
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_params_rand2_newargs(self):
        h2o.beta_features = True
        csvPathname = 'covtype/covtype.20k.data'
        hex_key = 'covtype.20k.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put')
        paramDict = define_params()
        for trial in range(20):
            # params is mutable. This is default.
            params = {'response': 54, 'lambda': 0, 'alpha': 0, 'n_folds': 1}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=70, parseResult=parseResult, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            h2o.check_sandbox_for_errors()
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
