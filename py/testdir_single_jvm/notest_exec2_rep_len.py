import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_exec as h2e, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=12)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_rep_len(self):

        for i in range(5):
            # have to make sure they're created as keys for reuse between execs
            expr1 = "a=rep_len(0,1000000)"
            expr2 = " b = runif(a,-1)"
            h2e.exec_expr(execExpr=expr1, timeoutSecs=180)
            h2e.exec_expr(execExpr=expr2)
            #execExpr = "b=a;;; d=a;;; f=a;;; g=a;;;"

            expr3 = "b = a"
            expr4 = "d = a"
            expr5 = "f = a"
            expr6 = "g = a"
            h2e.exec_expr(execExpr=expr3)
            h2e.exec_expr(execExpr=expr4)
            h2e.exec_expr(execExpr=expr5)
            h2e.exec_expr(execExpr=expr6)
            
            execExpr = "h <- cbind(a ,b)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)
            execExpr = "h <- cbind(a ,b, d)"
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=180)

        h2o.check_sandbox_for_errors()


if __name__ == '__main__':
    h2o.unit_main()
