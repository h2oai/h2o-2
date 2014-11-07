import unittest
import random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

# keep two lists the same size
# best if prime relative to the # jvms (len(h2o.nodes))

print "Test the order of aritmetic operations. Should match standard"
print "http://en.wikipedia.org/wiki/Order_of_operations"
print "Order of operators should be"
print "Parentheses, Exponents, Multiplication, Division, Addition, Subtraction"
# Different calculators follow different orders of operations. 
# Most non-scientific calculators without a stack work left to right without any priority 
# given to different operators, for example giving
#    1 + 2 \times 3 = 9, \;
# while more sophisticated calculators will use a more standard priority, for example giving
#    1 + 2 \times 3 = 7. \;
# The Microsoft Calculator program uses the former in its standard view and the latter 
# in its scientific and programmer views.

# Google has a nice calculator built into search. Just enter the expression and you get a nice 
# thing showing eval order with parens

# Many programming languages use precedence levels that conform to the order commonly used in mathematics, though some, such as APL and Smalltalk, have no operator precedence rules (in APL, evaluation is strictly right to left; in Smalltalk, it's strictly left to right).

# 2x/2x
# 2*x/2*x
# 2(x)/2(x) 

exprList = [
        '-1', 
        '(-1)', 
        '-(1)', 
        '--1', 
        '-(-1)', 
        '1/2',
        '1/2/3',
    ]

resultList = [
        '-1', 
        '-1', 
        '-1', 
        '1', 
        '1', 
        '0.5',
        '0.16666666666'
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3,java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_arith_precedence(self):
        lenNodes = len(h2o.nodes)
        trial = 0
        for (execExpr, expectedResult) in zip(exprList, resultList):
            resultKey = "R" + str(trial)
            (execResultInspect, min_value) = h2e.exec_expr(None, resultKey + '=' + execExpr, 
                resultKey=None, timeoutSecs=4)

            print "trial: #" + str(trial), min_value, execExpr
            print "min_value: %s  expectedResult: %s" % (min_value, expectedResult)
            self.assertAlmostEqual(float(min_value), float(expectedResult), places=6,
                msg="exec wrong answer %s %s" % (min_value, expectedResult) )
            trial += 1

if __name__ == '__main__':
    h2o.unit_main()
