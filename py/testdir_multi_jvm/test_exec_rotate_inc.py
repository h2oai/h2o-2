import unittest
import random, sys, time, os, re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

initList = [
        'Result0 = 0',
        'Result1 = 1',
        'Result2 = 2',
        'Result3 = 3',
        'Result4 = 4',
        'Result5 = 5',
        'Result6 = 6',
        'Result7 = 7',
        'Result8 = 8',
        'Result9 = 9',
        'Result10 = 10',
    ]

# NOTE. the inc has to match the goback used below
goback = 7
exprList = [
        'Result<n> = Result<m> + ' + str(goback),
    ]

def fill_in_expr_template(exprTemplate, n, m):
    execExpr = exprTemplate
    execExpr = re.sub('<n>',str(n),execExpr)
    execExpr = re.sub('<m>',str(m),execExpr)
    h2o.verboseprint("\nexecExpr:", execExpr)
    return execExpr

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
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_rotating_inc(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        # zero the list of Results using node[0]
        # FIX! is the zerolist not eing seen correctl? is it not initializing to non-zero?
        for exprTemplate in initList:
            execExpr = fill_in_expr_template(exprTemplate, '0', '0')
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
                if (trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                ### print nodeX
                
                number = trial + 10
                execExpr = fill_in_expr_template(exprTemplate, 
                    str(number%period), str((number-goback)%period))
                resultKey="Result" + str(number%period)
                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr,
                    resultKey=resultKey, timeoutSecs=4)

                print "min_value:", min_value, "execExpr:", execExpr, "number:", number
                h2o.verboseprint("min: ", min_value, "trial:", trial)
                self.assertEqual(int(min_value), int(number),
                    'Although the memory model allows write atomicity to be violated,' +
                    'this test was passing with an assumption of multi-jvm write atomicity' + 
                    'Be interesting if ever fails. Can disable assertion if so, and run without check')

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1

if __name__ == '__main__':
    h2o.unit_main()
