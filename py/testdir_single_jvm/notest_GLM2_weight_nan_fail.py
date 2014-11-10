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
        h2o.tear_down_cloud()

    def test_NOPASS_GLM2_weight_nan_fail(self):
        csvPathname = 'covtype/covtype.20k.data'
        hex_key = 'covtype.20k.hex'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, schema='put')
        kwargs = {
            'destination_key': 'GLM_model_python_0_default_0', 
            'family': 'tweedie', 
            'tweedie_variance_power': 1.9999999, 
            'max_iter': 10, 
            'alpha': 0, 
            'lambda': 0,
            'response': 54, 
        }

        for trial in range(3):
            # params is mutable. This is default.
            start = time.time()
            glm = h2o_cmd.runGLM(timeoutSecs=70, parseResult=parseResult, **kwargs)
            h2o.check_sandbox_for_errors()
            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "Trial #", trial, "completed\n"

if __name__ == '__main__':
    h2o.unit_main()
