import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm

def define_params():
    paramDict = {
        ## 'x': [0,1,15,33,34],
        'family': ['poisson'],
        'num_cross_validation_folds': [2,3,4,9],
        'thresholds': [0.1, 0.5, 0.7, 0.9],
        'lambda': [1e-8, 1e-4],
        'alpha': [0,0.5,0.75],
        # don't use defaults? they have issues?
        'beta_epsilon': [0.001, 0.0001],
        # case_mode not used for poisson?
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        # don't use defaults? they have issues?
        'max_iter': [5, 10, 19, 31],
        'weight': [None, 1, 2, 4],
        # new expert stuff
        'expert': [None,0,1],
        'lsm_solver': ['ADMM'],
        'standardize': [None, 0, 1],

        }
    return paramDict

def info_from_inspect(inspect, csvPathname):
    # need more info about this dataset for debug
    cols = inspect['cols']
    # look for nonzero num_missing_values count in each col
    for i, colDict in enumerate(cols):
        num_missing_values = colDict['num_missing_values']
        if num_missing_values != 0:
            ### print "%s: col: %d, num_missing_values: %d" % (csvPathname, i, num_missing_values)
            pass

    num_cols = inspect['num_cols']
    num_rows = inspect['num_rows']
    row_size = inspect['row_size']
    ptype = inspect['type']
    value_size_bytes = inspect['value_size_bytes']
    response = inspect['response']
    ptime = response['time']

    print "num_cols: %s, num_rows: %s, row_size: %s, ptype: %s, \
           value_size_bytes: %s, response: %s, time: %s" % \
           (num_cols, num_rows, row_size, ptype, value_size_bytes, response, ptime)

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
        info_from_inspect(inspect, csvPathname)

        # for determinism, I guess we should spit out the seed?
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        paramDict = define_params()
        print "\nUsing random seed:", SEED
        for trial in range(20):
            # params is mutable. This is default.
            # FIX! does it never end if we don't have alpha specified?
            params = {
                'y': 6, 
                'num_cross_validation_folds': 3, 
                'family': "poisson", 
                'alpha': 0.5, 
                'lambda': 1e-4, 
                'beta_eps': 0.001, 
                'max_iter': 30
                }

            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            # make timeout bigger with xvals
            timeoutSecs = 60 + (kwargs['num_cross_validation_folds']*20)
            # or double the 4 seconds per iteration (max_iter+1 worst case?)
            timeoutSecs = max(timeoutSecs, (8 * (kwargs['max_iter']+1)))

            start = time.time()
            glm = h2o_cmd.runGLMOnly(timeoutSecs=timeoutSecs, parseKey=parseKey, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            start = time.time()
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "simpleCheckGLM end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
