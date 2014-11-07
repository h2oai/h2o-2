import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_glm, h2o_import as h2i, h2o_exec as h2e

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(2)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM2_covtype_single_cols(self):
        timeoutSecs = 120
        csvPathname = 'standard/covtype.data'
        print "\n" + csvPathname

        # columns start at 0
        y = 54
        ignore_x = ""
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', 
            hex_key='A.hex', timeoutSecs=15)

        case = 2
        execExpr="A.hex[,%s]=(A.hex[,%s]==%s)" % (y+1, y+1, case)
        h2e.exec_expr(execExpr=execExpr, timeoutSecs=30)

        print "GLM binomial ignoring 1 X column at a time" 
        print "Result check: abs. value of coefficient and intercept returned are bigger than zero"
        for colX in xrange(1,53):
            if ignore_x == "": 
                ignore_x = 'C' + str(colX)
            else:
                # x = x + "," + str(colX)
                ignore_x = 'C' + str(colX)

            sys.stdout.write('.')
            sys.stdout.flush() 
            print "y:", y

            start = time.time()
            kwargs = {'ignored_cols': ignore_x, 'response': y, 'n_folds': 6 }
            glm = h2o_cmd.runGLM(parseResult={'destination_key': 'A.hex'}, timeoutSecs=timeoutSecs, **kwargs)
            h2o_glm.simpleCheckGLM(self, glm, None, **kwargs)
            print "glm end on ", csvPathname, 'took', time.time() - start, 'seconds'

if __name__ == '__main__':
    h2o.unit_main()
