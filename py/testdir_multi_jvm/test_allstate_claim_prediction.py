import unittest, os, sys, time
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()
        ## h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_claim_prediction_gaussian(self):
        csvPathname = 'allstate/claim_prediction_train_set_10000_int.csv.gz'
        kwargs = {'family': 'gaussian', 'y': 'Claim_Amount', 'alpha': 0, 'lambda': 0.5, 'max_iter': 15}
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=150, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

    def test_B_claim_prediction_binomial(self):
        csvPathname = 'allstate/claim_prediction_train_set_10000_int.csv.gz'
        kwargs = {'family': 'binomial', 'y': 'Claim_Amount', 'case_mode': '>', 'case': 100, 'alpha': 0, 'lambda': 0.5, 'max_iter': 15}
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=150, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

    def test_C_claim_prediction_binomial(self):
        print "nan/fail to solve if alpha=0/lambda=0. Using alpha=0/lambda=0.5"
        csvPathname = 'allstate/claim_prediction_train_set_10000_int.csv.gz'
        kwargs = {'family': 'poisson', 'y': 'Claim_Amount', 'alpha': 0, 'lambda': 0.5, 'max_iter': 15}
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=150, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main() 
