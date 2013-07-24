import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json
import h2o, h2o_cmd, h2o_hosts, h2o_glm

# none is illegal for threshold
# always run with n_folds, to make sure we get the trainingErrorDetails
# FIX! we'll have to do something for gaussian. It doesn't return the ted keys below

# some newer args for new port
# made this a separate test, so the coarse failures don't impede other random testing
def define_params(): 
    paramDict = { # FIX! no ranges on X 0:3
        'standardize': [None, 0,1],
        'lsm_solver': [None, 'AUTO','ADMM','GenGradient'],
        'beta_epsilon': [None, 0.0001],
        'expert': [None, 0, 1],
        'thresholds': [None, 0.1, 0.5, 0.7, 0.9],

        'x': [0,1,15,33],
        'family': ['gaussian', 'binomial'],
        'lambda': [0, 1e-8, 1e-4],
        'alpha': [0,0.2,0.8],
        'case': [1,2,3,4,5,6],
        'case_mode': ['>','<','=','<=','>='],
        'link': [None, 'logit'],
        'max_iter': [None, 10],
        'weight': [None, 1, 2, 4],
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

    def test_GLM_params_rand2_newargs(self):
        # csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
        key = 'covtype.20k'
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key=key)
        paramDict = define_params()
        for trial in range(20):
            # params is mutable. This is default.
            params = {'y': 54, 'case': 1, 'lambda': 0, 'alpha': 0, 'n_folds': 1}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            glm = h2o_cmd.runGLMOnly(timeoutSecs=70, parseKey=parseKey, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            h2o.check_sandbox_for_errors()
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
