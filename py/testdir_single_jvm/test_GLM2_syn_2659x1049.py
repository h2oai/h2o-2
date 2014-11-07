import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

params = {
    'response': 1049, 
    'family': 'binomial', 
    'beta_epsilon': 0.0001, 
    'alpha': 1.0, 
    'lambda': 1e-05,
    'n_folds': 1, 
    'max_iter': 20, 
}
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_syn_2659x1049(self):
        csvFilename = "syn_2659x1049.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        kwargs = params
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=120, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

    def test_GLM2_syn_2659x1049x2enum(self):
        csvFilename = "syn_2659x1049x2enum.csv"
        csvPathname = 'logreg' + '/' + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=csvFilename + ".hex", schema='put')
        kwargs = params
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=240, **kwargs)
        h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
