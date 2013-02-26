import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

initList = [
        'Result.hex = 0',
        'Result.hex = 1',
        'Result.hex = 2',
        'Result.hex = 3',
        'Result.hex = 4',
        'Result.hex = 5',
        'Result.hex = 6',
        'Result.hex = 7',
        'Result.hex = 8',
        'Result.hex = 9',
        'Result.hex = 10',
    ]

exprList = [
        'Result.hex = Result.hex + 1',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_result_race(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        # zero the list of Results using node[0]
        # FIX! is the zerolist not eing seen correctl? is it not initializing to non-zero?
        for execExpr in initList:
            h2e.exec_expr(h2o.nodes[0], execExpr, resultKey="Result.hex", timeoutSecs=4)
            ### print "\nexecResult:", execResult

        trial = 0
        while (trial < 200):
            for exprExpr in exprList:
                # for the first 100 trials: do each expression at node 0,
                # for the second 100 trials: do each expression at a random node, to facilate key movement
                # FIX! there's some problem with the initList not taking if rotated amongst nodes?
                if (trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                
                resultKey = "Result.hex"
                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr,
                    resultKey=resultKey, timeoutSecs=4)

                print min_value, execExpr
                h2o.verboseprint("min_value: ", min_value, "trial:", trial)

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1

if __name__ == '__main__':
    h2o.unit_main()
