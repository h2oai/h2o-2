import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

def define_params():
    paramDict = {
        'standardize': [None, 0,1],
        'lsm_solver': [None, 'AUTO','ADMM','GenGradient'],
        'expert_settings': [None, 0, 1],
        'thresholds': [None, 0.1, 0.5, 0.7, 0.9],
        'x': [0,1,15,33,34],
        'family': ['binomial'],
        'n_folds': [2,3],
        'thresholds': [0.1, 0.5, 0.7, 0.9],
        'lambda': [1e-8, 1e-4],
        'alpha': [0,0.5,0.75],
        'beta_epsilon': [0.001, 0.0001],
        'case': [1,2,3,4,5,6],
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        'max_iter': [10,19,31],
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

    def test_GLM_params_rand2_4082088627997819015(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key='covtype.hex')
        paramDict = define_params()
        for trial in range(40):
            # params is mutable. This is default.
            params = {
                'y': 54, 
                'n_folds' : 3, 
                'family' : 'binomial', 
                'max_iter' : 5, 
                'case': 1, 
                'alpha': 0, 
                'lambda': 0
            }
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()
            start = time.time()
            timeoutSecs = max(150, params['n_folds']*10 + params['max_iter']*10)
            glm = h2o_cmd.runGLM(timeoutSecs=timeoutSecs, parseResult=parseResult, **kwargs)
            elapsed = time.time() - start
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            # FIX! I suppose we have the problem of stdout/stderr not having flushed?
            # should hook in some way of flushing the remote node stdout/stderr
            h2o.check_sandbox_for_errors()
            
            print "glm end on ", csvPathname, 'took', elapsed, 'seconds.',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
