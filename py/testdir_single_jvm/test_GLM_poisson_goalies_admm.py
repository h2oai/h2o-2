import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

def define_params():

    print "Always using standardize=1. Unable to solve sometimes if not?"
    paramDict = {
        ## 'x': [0,1,15,33,34],
        'family': ['poisson'],
        'num_cross_validation_folds': [1],
        'thresholds': [0.5],
        'lambda': [0,1e-8],
        'alpha': [0],
        # don't use defaults? they have issues?
        'beta_epsilon': [0.001, 0.0001],
        # case_mode not used for poisson?
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        # don't use defaults? they have issues?
        'max_iter': [3],
        'weight': [None, 1, 2, 4],
        # new expert stuff
        'expert': [None,0,1],
        'lsm_solver': ['ADMM'],
        'standardize': [1],

        }
    return paramDict

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_loop_random_param_covtype(self):
        csvPathname = h2o.find_file('smalldata/poisson/Goalies.csv')
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)
        inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

        # need more info about the dataset for debug
        h2o_cmd.info_from_inspect(inspect, csvPathname)

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED =
        random.seed(SEED)
        paramDict = define_params()
        print "\nUsing random seed:", SEED
        for trial in range(5):
            # params is mutable. This is default.
            # FIX! does it never end if we don't have alpha specified?
            params = {
                'y': 5,
                'num_cross_validation_folds': 1,
                'family': "poisson",
                'alpha': 0.0,
                # 'lambda': 1e-8,
                'lambda': 0,
                'beta_epsilon': 0.001,
                'max_iter': 3,
                'standardize': 1,
                'expert': 1,
                'lsm_solver': 'ADMM',
                }

            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            # make timeout bigger with xvals
            timeoutSecs = 120 + (kwargs['num_cross_validation_folds']*30)
            # or double the 4 seconds per iteration (max_iter+1 worst case?)
            timeoutSecs = max(timeoutSecs, (8 * (kwargs['max_iter']+1)))

            start = time.time()
            print "May not solve. Expanded categorical columns causing a large # cols, small # of rows"
            glm = h2o_cmd.runGLMOnly(timeoutSecs=timeoutSecs, parseKey=parseKey, **kwargs)
            elapsed = time.time()-start
            print "glm end on ", csvPathname, "Trial #", trial, "completed in", elapsed, "seconds.",\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            start = time.time()
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "simpleCheckGLM end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
