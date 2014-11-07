import unittest, time, sys
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_big1(self):
        csvPathname = 'hhp_107_01.data.gz'
        print "\n" + csvPathname

        y = "106"
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, timeoutSecs=15, schema='put')

        for trial in xrange(3):
            sys.stdout.write('.')
            sys.stdout.flush() 
            print "response:", y

            start = time.time()
            kwargs = {'response': y, 'n_folds': 6, 'alpha': 0.0}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=300, **kwargs)

            # pass the kwargs with all the params, so we know what we asked for!
            h2o_glm.simpleCheckGLM(self, glm, "C58", **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\nTrial #", trial

if __name__ == '__main__':
    h2o.unit_main()
