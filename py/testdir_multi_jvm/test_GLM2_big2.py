import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2,java_heap_GB=5)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_big2(self):
        csvPathname = "hhp_107_01.data.gz"
        print "\n" + csvPathname
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put', timeoutSecs=15)
        ## h2b.browseTheCloud()

        y = "106"
        # go right to the big X and iterate on that case
        ### for trial in range(2):
        for trial in range(2):
            print "\nTrial #", trial, "start"

            start = time.time()
            kwargs = {'response': y, 'alpha': 0.0}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=200, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, 'C58', **kwargs)
            h2o.check_sandbox_for_errors()
            ### h2b.browseJsonHistoryAsUrlLastMatch("GLM")
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
