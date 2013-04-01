import unittest
import random, sys, time
sys.path.extend(['.','..','py'])
import json

import h2o, h2o_cmd, h2o_hosts, h2o_glm

def define_params():
    paramDict = {
        'family': [None, 'gaussian', 'binomial', 'poisson'],
        'num_cross_validation_folds': [2,3,4,9],
        'thresholds': [0.1, 0.5, 0.7, 0.9],
        # seem to get zero coeffs with lamba=1 case=7
        # just keep the range smaller for this dataset
        'lambda': [0,1e-8,1e-4,1e-3],
        'alpha': [0,0.8,0.75],
        # new?
        'beta_epsilon': [None, 0.0001],
        # eliminate case=7 because it's so rare, especially with cross validation
        'case': [1,2,3,4,5,6],
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        # 'link': [None, 'logit','identity'],
        # This is the new name? fine, we don't care for old or old testing (maxIter?)
        'max_iter': [None, 10],
        'weight': [None, 1, 2, 4],
        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_params_rand2(self):
        # csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        csvPathname = h2o.find_file('smalldata/covtype/covtype.20k.data')
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key="covtype.20k")

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED =
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        paramDict = define_params()
        for trial in range(20):
            # params is mutable. This is default.
            params = {'y': 54, 'case': 1, 'alpha': 0, 'lambda': 0}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            glm = h2o_cmd.runGLMOnly(timeoutSecs=70, parseKey=parseKey, **kwargs)
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
