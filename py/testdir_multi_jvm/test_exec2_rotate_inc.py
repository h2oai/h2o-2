import unittest
import random, sys, time, os, re
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e


DO_ONE_NODE_ONLY = True
initList = [
        'Result0 = c(0)',
        'Result1 = c(1)',
        'Result2 = c(2)',
        'Result3 = c(3)',
        'Result4 = c(4)',
        'Result5 = c(5)',
        'Result6 = c(6)',
        'Result7 = c(7)',
        'Result8 = c(8)',
        'Result9 = c(9)',
        'Result10 = c(10)',
    ]

# NOTE. the inc has to match the goback used below
goback = 7
exprList = [
        'Result<n> = Result<m> + ' + str(goback),
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
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_rotate_inc(self):

        lenNodes = len(h2o.nodes)
        # zero the list of Results using node[0]
        # FIX! is the zerolist not eing seen correctl? is it not initializing to non-zero?
        for exprTemplate in initList:
            execExpr = h2e.fill_in_expr_template(exprTemplate, n=0, m=0)
            print execExpr
            execResult = h2e.exec_expr(h2o.nodes[0], execExpr)
            ### print "\nexecResult:", execResult

        period = 10
        # start at result10, to allow goback of 10
        trial = 0
        while (trial < 200):
            for exprTemplate in exprList:
                # for the first 100 trials: do each expression at node 0,
                # for the second 100 trials: do each expression at a random node, to facilate key movement
                # FIX! there's some problem with the initList not taking if rotated amongst nodes?
                if (DO_ONE_NODE_ONLY or trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                ### print nodeX
                
                number = trial + 10
                resultKey="Result" + str(number%period)
                execExpr = h2e.fill_in_expr_template(exprTemplate, n=(number%period), m=((number-goback)%period))
                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr,
                    resultKey=None, timeoutSecs=4)

                print "min_value:", min_value, "execExpr:", execExpr, "number:", number
                h2o.verboseprint("min: ", min_value, "trial:", trial)
                self.assertEqual(int(min_value), int(number))
                # we're talking to just one node. ignore this comment
                #    'Although the memory model allows write atomicity to be violated,' +
                #    'this test was passing with an assumption of multi-jvm write atomicity' + 
                #    'Be interesting if ever fails. Can disable assertion if so, and run without check')
#
                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1

if __name__ == '__main__':
    h2o.unit_main()
