import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

def define_params():
    paramDict = {
        'standardize': [None, 0,1],
        'lsm_solver': [None, 'AUTO','ADMM','GenGradient'],
        'beta_epsilon': [None, 0.0001],
        'expert_settings': [None, 0, 1],

        'thresholds': [None, 0.1, 0.5, 0.7, 0.9],
        'x': [0,1,15,33,34],
        'family': ['gamma'],
        'n_folds': [2,3,4],
        'lambda': [1e-8, 1e-4],
        'alpha': [0,0.5,0.75],
        'case': [1,2,3,4,5,6,7],
        # inverse and log causing problems
        # 'link': [None, 'logit','identity', 'log', 'inverse'],
        'max_iter': [24],
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

    def test_GLM_gamma_rand2(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        paramDict = define_params()
        for trial in range(10):
            # params is mutable. This is default.
            params = {'y': 54, 'n_folds': 3, 'family': "gamma", 'alpha': 0.5, 'lambda': 1e-4, 'max_iter': 24}
            colX = h2o_glm.pickRandGlmParams(paramDict, params)
            kwargs = params.copy()

            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=300, parseResult=parseResult, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
