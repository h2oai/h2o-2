import unittest, random, sys, time, os
sys.path.extend(['.','..','../..','py'])

import h2o, h2o_browse as h2b, h2o_exec as h2e

initList = [
        'Result.hex = c(0)',
        'Result.hex = c(1)',
        'Result.hex = c(2)',
        'Result.hex = c(3)',
        'Result.hex = c(4)',
        'Result.hex = c(5)',
        'Result.hex = c(6)',
        'Result.hex = c(7)',
        'Result.hex = c(8)',
        'Result.hex = c(9)',
        'Result.hex = c(10)',
    ]

exprList = [
        'Result.hex = Result.hex + 1',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_result_race(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        # zero the list of Results using node[0]
        # FIX! is the zerolist not eing seen correctl? is it not initializing to non-zero?
        for execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey="Result.hex", timeoutSecs=20)
            ### print "\nexecResult:", execResult

        trial = 0
        while (trial < 200):
            for execExpr in exprList:
                # for the first 100 trials: do each expression at node 0,
                # for the second 100 trials: do each expression at a random node, to facilate key movement
                # FIX! there's some problem with the initList not taking if rotated amongst nodes?
                if (trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                
                resultKey = "Result.hex"
                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr,
                    resultKey=resultKey, timeoutSecs=20)

                print min_value, execExpr
                h2o.verboseprint("min_value: ", min_value, "trial:", trial)

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1

if __name__ == '__main__':
    h2o.unit_main()
