import unittest, random, sys, time
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_browse as h2b, h2o_exec as h2e, h2o_import as h2i, h2o_cmd, h2o_util

import rpy2.robjects as robjects
import h2o_eqns
import math

print "run some random expressions (using h2o_eqn.py) in exec and R and compare results (eventually)"
exprList = [
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(1, java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_expr_rpy2(self):

        for k in range(20):
            a = random.randint(1,10)
            # b = random.randint(49,50)
            b = random.randint(1,10)
            c = random.randint(0,3)
            for k in range(50):
                execExpr = "a=" + str(h2o_eqns.Expression(a, b, c)) + ";"
                (resultExec, hResult) = h2e.exec_expr(execExpr=execExpr)
                print "h2o:", hResult

                rResult = robjects.r(execExpr)[0]
                print "R:", rResult

                if math.isinf(rResult):
                    # covers pos/neg inf?
                    if not 'Infinity' in str(hResult):
                        raise Exception("h2o: %s R: %s not equal" % (hResult, rResult))
                elif math.isnan(rResult):
                    if not 'NaN' in str(hResult):
                        raise Exception("h2o: %s R: %s not equal" % (hResult, rResult))
                elif 'Infinity' in str(hResult) or'NaN' in str(hResult):
                        raise Exception("h2o: %s R: %s not equal" % (hResult, rResult))
                else:
                    # skip Inf
                    # don't do logicals..h2o 1/0, R True/False
                    h2o_util.assertApproxEqual(rResult, hResult, tol=1e-12, msg='mismatch h2o/R expression result')


if __name__ == '__main__':
    h2o.unit_main()



