import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_GLM2_gamma_fail1(self):
        csvPathname = 'standard/covtype.data'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put')
        for trial in range(5):
            kwargs = {
                'standardize': 1, 
                'family': 'gamma', 
                'response': 54, 
                'lambda': 0.0001,
                'alpha': 0.5, 
                'max_iter': 25, 
                'n_folds': 1, 
            }
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=120, parseResult=parseResult, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

            # if we hit the max_iter, that means it probably didn't converge. should be 1-maxExpectedIter
            # h2o_glm.simpleCheckGLM(self, glm, None, maxExpectedIterations=kwargs['max_iter']-2, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, None, **kwargs)
            print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
