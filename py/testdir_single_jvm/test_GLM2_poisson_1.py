import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(1,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_poisson_1(self):
        csvFilename = 'covtype.data'
        csvPathname = 'standard/' + csvFilename
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', timeoutSecs=20)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\n" + csvPathname, \
            "    numRows:", "{:,}".format(inspect['numRows']), \
            "    numCols:", "{:,}".format(inspect['numCols'])

        print "WARNING: max_iter set to 8 for benchmark comparisons"
        max_iter = 8

        y = "54"
        kwargs = {
            'response': y,
            'family': 'poisson',
            'n_folds': 0,
            'max_iter': max_iter,
            'beta_epsilon': 1e-3}

        timeoutSecs = 120
        # L2 
        start = time.time()
        kwargs.update({'alpha': 0, 'lambda': 0})
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L2) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, "C14", **kwargs)

        # Elastic
        kwargs.update({'alpha': 0.5, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (Elastic) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, "C14", **kwargs)

        # L1
        kwargs.update({'alpha': 0.75, 'lambda': 1e-4})
        start = time.time()
        glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
        print "glm (L1) end on ", csvPathname, 'took', time.time() - start, 'seconds'
        h2o_glm.simpleCheckGLM(self, glm, "C14", **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
