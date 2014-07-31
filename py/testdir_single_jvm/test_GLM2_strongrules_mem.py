import unittest, sys, time
sys.path.extend(['.','..','py'])
import h2o_hosts, h2o_glm
import h2o, h2o_cmd, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_strongrules_mem(self):
        csvFilename = 'arcene_train.data'
        csvPathname = 'arcene'+'/' + csvFilename
        noStrongRules = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        #noStrongRules = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=150)
        params = {
                    'alpha': 0.5,
                    'n_folds': 0,
                    'strong_rules_enabled': 0,
                    'lambda_search': 0,
                    'response': 0, 
                    'cols': [i for i in range(1,5000)],
                    'ignored_cols': [i for i in range(5000,10000)], 
                    'family': 'binomial',
                }
        kwargs = params.copy()
        starttime = time.time()
        glmtest = h2o_cmd.runGLM(parseResult=noStrongRules, **kwargs)
        #glmtest = h2o_cmd.runGLM(parseResult=noStrongRules, timeoutSecs=100, **kwargs)
        elapsedtime = time.time() - starttime 
        print("GLM runtime: ",elapsedtime)
        h2o_glm.simpleCheckGLM(self, glmtest, None, **kwargs)


if __name__ == '__main__':
    h2o.unit_main()
























