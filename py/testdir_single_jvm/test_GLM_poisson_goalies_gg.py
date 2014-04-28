import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

def define_params():

    print "Always using standardize=1. Unable to solve sometimes if not?"
    paramDict = {
        'standardize': [1],
        'lsm_solver': ['GenGradient'],
        'beta_epsilon': [None, 0.0001],
        'expert_settings': [1],
        'thresholds': [None, 0.1, 0.5, 0.7, 0.9],

        'family': ['poisson'],
        'n_folds': [1],
        'thresholds': [0.5],
        'lambda': [0,1e-8],
        'alpha': [0],
        'max_iter': [3],

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

    def test_GLM_poisson_goalies_gg(self):
        csvPathname = 'poisson/Goalies.csv'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        h2o_cmd.infoFromInspect(inspect, csvPathname)
        paramDict = define_params()
        for trial in range(5):
            # params is mutable. This is default.
            # FIX! does it never end if we don't have alpha specified?
            params = {
                'y': 5,
                'n_folds': 1,
                'family': "poisson",
                'alpha': 0.0,
                'lambda': 0,
                'beta_epsilon': 0.001,
                'max_iter': 3,
                'standardize': 1,
                'expert_settings': 1,
                'lsm_solver': 'GenGradient',
                }

            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            # make timeout bigger with xvals
            timeoutSecs = 180 + (kwargs['n_folds']*30)
            # or double the 4 seconds per iteration (max_iter+1 worst case?)
            timeoutSecs = max(timeoutSecs, (8 * (kwargs['max_iter']+1)))

            start = time.time()
            print "May not solve. Expanded categorical columns causing a large # cols, small # of rows"
            glm = h2o_cmd.runGLM(timeoutSecs=timeoutSecs, parseResult=parseResult, **kwargs)
            elapsed = time.time()-start
            print "glm end on ", csvPathname, "Trial #", trial, "completed in", elapsed, "seconds.",\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            start = time.time()
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "simpleCheckGLM end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
