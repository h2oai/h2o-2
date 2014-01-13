import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype_single_cols(self):
        timeoutSecs = 10
        csvPathname = 'standard/covtype.data'
        print "\n" + csvPathname

        # columns start at 0
        y = "54"
        x = ""
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', timeoutSecs=15)

        print "GLM binomial wth 1 X column at a time" 
        print "Result check: abs. value of coefficient and intercept returned are bigger than zero"
        for colX in xrange(54):
            if x == "": 
                x = str(colX)
            else:
                # x = x + "," + str(colX)
                x = str(colX)

            sys.stdout.write('.')
            sys.stdout.flush() 
            print "\nx:", x
            print "y:", y

            start = time.time()
            kwargs = {'x': x, 'y': y, 'n_folds': 6, 'case': 2}
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, colX, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'


if __name__ == '__main__':
    h2o.unit_main()
