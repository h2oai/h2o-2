import unittest, random, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_import as h2i

def define_params():
    paramDict = {
        'standardize': [None, 0,1],
        'lsm_solver': [None, 'AUTO','ADMM','GenGradient'],
        'beta_epsilon': [None, 0.0001],
        'expert_settings': [None, 0, 1],
        'family': [None, 'gaussian', 'binomial', 'poisson'],
        'thresholds': [None, 0.1, 0.5, 0.7, 0.9],
        # seem to get zero coeffs with lamba=1 case=7
        # just keep the range smaller for this dataset
        'lambda': [0,1e-8,1e-4,1e-3],
        'alpha': [0,0.8,0.75],
        # eliminate case=7 because it's so rare, especially with cross validation
        'case': [1,2,3,4,5,6],
        'max_iter': [None, 10],
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

    def test_GLM_params_rand2(self):
        csvPathname = 'covtype/covtype.20k.data'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', hex_key="covtype.20k")
        paramDict = define_params()
        for trial in range(20):
            # params is mutable. This is default.
            params = {'y': 54, 'case': 1, 'alpha': 0, 'lambda': 0, 'n_folds': 1}
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
