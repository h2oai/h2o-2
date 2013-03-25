import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_hosts

print "Repeated test of case that timeouts in EC2"

def define_params():
    paramDict = {
        'y': 54,
        'weight': None, 
        'family': 'poisson', 
        'beta_epsilon': 0.001, 
        'thresholds': 0.1, 
        'max_iter': 15, 
        'link': 'familyDefault', 
        'alpha': 0.5, 
        'num_cross_validation_folds': 9, 
        'lambda': 1e-4
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

    def test_loop_random_param_covtype(self):
        start = time.time()
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/covtype.data')
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname)
        print "upload/parse end on ", csvPathname, 'took', time.time() - start, 'seconds'

        kwargs = define_params()
        for trial in range(3):
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
